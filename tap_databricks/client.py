"""SQL client handling.

This includes DatabricksStream and DatabricksConnector.
"""

from __future__ import annotations

import functools
import sys
from typing import TYPE_CHECKING, Any

from singer_sdk.helpers.conform import TypeConformanceLevel
from singer_sdk.sql import SQLConnector, SQLStream
from singer_sdk.sql.connector import SQLToJSONSchema

import sqlalchemy
from sqlalchemy import text
from sqlalchemy import URL, Engine
from sqlalchemy.engine.reflection import ObjectKind
from databricks.sdk.core import Config, oauth_service_principal

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    import sqlalchemy
    from singer_sdk.helpers.types import Context, Record
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector


class DatabricksSQLToJSONSchema(SQLToJSONSchema):
    """Custom SQL to JSON Schema conversion for Databricks.

    Developers should override this class to customize how SQL types are converted
    to JSON Schema types. This is particularly useful for databases with custom
    or non-standard SQL types.
    """

    def __init__(
        self,
        *,
        custom_config_option: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize the SQL to JSON Schema converter.

        Args:
            custom_config_option: Example custom configuration option.
            **kwargs: Additional keyword arguments passed to parent class.
        """
        super().__init__(**kwargs)
        self.custom_config_option = custom_config_option

    @override
    @classmethod
    def from_config(cls, config: Mapping) -> DatabricksSQLToJSONSchema:
        """Instantiate the SQL to JSON Schema converter from a config dictionary.

        Developers should override this method to pass custom configuration options
        from the tap's config to the converter.

        Args:
            config: The tap's configuration dictionary.

        Returns:
            An instance of the SQL to JSON Schema converter.
        """
        return cls(
            custom_config_option=config.get("custom_config_option", False),
        )

    @override
    @functools.singledispatchmethod
    def to_jsonschema(self, column_type: Any) -> dict:
        """Customize the JSON Schema for Databricks types.

        Developers should not need to override this base method. Instead, register
        specific type handlers using the @to_jsonschema.register decorator for
        specific SQLAlchemy column types.

        Args:
            column_type: The SQLAlchemy column type to convert.

        Returns:
            A JSON Schema type definition.
        """
        return super().to_jsonschema(column_type)

    # Example: Custom type conversion for database-specific types
    # @to_jsonschema.register
    # def custom_type_to_jsonschema(self, column_type: CustomSQLType) -> dict:
    #     """Override the default mapping for CustomSQLType columns.
    #
    #     Developers may add custom type mappings by registering handlers
    #     for specific SQLAlchemy column types.
    #     """
    #     if self.custom_config_option:
    #         return {"type": ["string", "null"]}
    #     return {"type": ["object", "null"], "additionalProperties": True}


# ---- module-level constant: edit this list to add/reorder candidates ----
# Column names (lowercase) auto-detected as replication keys, in PRIORITY order.
# First match wins. Suggest-only: we set replication-key, never the method.
REPLICATION_KEY_CANDIDATES = [
    "lastmodifieddate",
    "systemmodstamp",
    "last_modified_date",
    "updated_at",
    "modified_at",
    "_synced_at",
]


class DatabricksConnector(SQLConnector):
    """Connects to the Databricks SQL source.

    This class handles all SQL connection logic and DDL operations.
    Developers may override methods to customize connection behavior,
    schema discovery, and type conversion.
    """

    # Custom SQL to JSON Schema converter
    sql_to_jsonschema_converter = DatabricksSQLToJSONSchema

    @override
    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source.

        Developers must implement this method to return a valid SQLAlchemy
        connection string for your specific database.

        Args:
            config: A dict with connection parameters

        Returns:
            SQLAlchemy connection string
        """
        query = {"http_path": config["http_path"], "catalog": config["catalog"]}
        if config.get("schema"):
            query["schema"] = config["schema"]
        if config.get("access_token"):  # PAT rides in the URL
            return URL.create(
                "databricks", username="token",
                password=config["access_token"],
                host=config["server_hostname"], query=query,
            ).render_as_string(hide_password=False)
        return URL.create(  # SP: no creds in URL, injected in create_engine
            "databricks", host=config["server_hostname"], query=query,
        ).render_as_string(hide_password=False)

    @override
    def create_engine(self) -> Engine:
        config = self.config
        if config.get("access_token"):  # PAT-first
            return sqlalchemy.create_engine(self.sqlalchemy_url)
        if config.get("client_id") and config.get("client_secret"):  # SP fallback
            def credential_provider():
                return oauth_service_principal(Config(
                    host=f"https://{config['server_hostname']}",
                    client_id=config["client_id"],
                    client_secret=config["client_secret"],
                ))
            return sqlalchemy.create_engine(
                self.sqlalchemy_url,
                connect_args={"credentials_provider": credential_provider},
            )
        raise ValueError("Provide either access_token (PAT) or client_id + client_secret (M2M).")

    @override
    def get_schema_names(self, engine: Engine, inspected: Inspector) -> list[str]:
        """Return a list of schema names in DB, or overrides with user-provided values.

        Developers may override this method to customize schema discovery,
        such as filtering out system schemas or applying user-defined filters.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine

        Returns:
            List of schema names
        """
        if self.config.get("schema"):
            return [self.config["schema"]]
        return super().get_schema_names(engine, inspected)

    @override
    def to_jsonschema_type(
        self,
        sql_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class. For more complex type
        conversion needs, consider overriding the sql_to_jsonschema_converter class.

        Args:
            sql_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return super().to_jsonschema_type(sql_type)

    @override
    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            jsonschema_type: A dict

        Returns:
            SQLAlchemy type
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return super().to_sql_type(jsonschema_type)
    
    # ---------- A + B: enrich discovery ----------
    @override
    def discover_catalog_entries(self, *, exclude_schemas=(), reflect_indices=True) -> list[dict]:
        """Resilient discovery: bulk reflection first, per-table fallback on failure.

        - PK bulk failure: no fallback (keys come from information_schema via A).
        - Column bulk failure: per-table fallback; a table whose columns can't be
          reflected is DROPPED (columns are required to build a stream).
        - Index bulk failure: per-table fallback; a table whose indices can't be
          reflected is KEPT (indices are optional; just no inferred keys).
        Then A/B enrichment runs on the survivors.
        """
        result: list[dict] = []
        dropped: list[str] = []          # tables dropped (column failure)
        index_skipped: list[str] = []    # tables kept but with no index reflection
        engine = self._engine
        inspected = sqlalchemy.inspect(engine)
        object_kinds = (
            (ObjectKind.TABLE, False),
            (ObjectKind.ANY_VIEW, True),
        )

        for schema_name in self.get_schema_names(engine, inspected):
            if schema_name in exclude_schemas:
                continue

            primary_keys = self._safe_multi_pk(inspected, schema_name)
            indices = (
                self._safe_multi_indexes(inspected, schema_name, index_skipped)
                if reflect_indices else {}
            )

            for object_kind, is_view in object_kinds:
                columns = self._safe_multi_columns(inspected, schema_name, object_kind, dropped)
                for schema, table in columns:
                    cols = columns[(schema, table)]
                    if not cols:  # column reflection failed for this table -> drop
                        continue
                    entry = self.discover_catalog_entry(
                        engine, inspected, schema, table, is_view,
                        reflected_columns=cols,
                        reflected_pk=primary_keys.get((schema, table)),
                        reflected_indices=indices.get((schema, table), []),
                    ).to_dict()
                    result.append(entry)

        # A/B enrichment on survivors
        pk_map = self._fetch_primary_keys()
        for entry in result:
            self._enrich_entry(entry, pk_map)
        
        no_key = [e["tap_stream_id"] for e in result if not e.get("key_properties")]
        if no_key:
            self.logger.info(
                "%d of %d discovered streams have no primary key. Targets cannot "
                "dedupe/upsert without one — set `table-key-properties` in "
                "meltano.yml (or the catalog) for those streams if needed.",
                len(no_key), len(result),
            )

        self.logger.info(
            "Discovery complete: %d streams discovered. "
            "Dropped %d tables due to column reflection failures "
            "(columns are required to build a stream). "
            "Retained %d tables whose index reflection failed "
            "(indices are optional; no inferred keys). "
            "See per-table warnings above for names.",
            len(result), len(dropped), len(index_skipped),
        )
        return result

    # ---------- resilient bulk-reflection wrappers (item E) ----------
    def _safe_multi_pk(self, inspected, schema_name: str) -> dict:
        """Bulk PK reflection; NO per-table fallback (A sources keys from
        information_schema, and per-table PK reflection re-triggers the same
        DESCRIBE TABLE EXTENDED that fails)."""
        try:
            return inspected.get_multi_pk_constraint(schema=schema_name)
        except Exception as e:  # noqa: BLE001
            self.logger.warning(
                "Bulk PK reflection failed for schema `%s`; keys will come from "
                "information_schema. BUT inspect the error to decide if the table/schema is usable"
                "Error: %s", schema_name, e,
            )
            return {}

    def _safe_multi_columns(self, inspected, schema_name: str, object_kind, dropped: list) -> dict:
        """Bulk column reflection; per-table fallback. A table that fails
        per-table gets an empty column list (caller drops it)."""
        try:
            return inspected.get_multi_columns(schema=schema_name, kind=object_kind)
        except Exception as e:  # noqa: BLE001
            self.logger.warning(
                "Bulk column reflection failed for schema `%s`; falling back to "
                "per-table. Error: %s", schema_name, e,
            )
        # per-table fallback
        out: dict = {}
        try:
            names = inspected.get_table_names(schema=schema_name)
        except Exception:  # noqa: BLE001
            names = []
        for table in names:
            try:
                out[(schema_name, table)] = inspected.get_columns(table, schema=schema_name)
            except Exception as e:  # noqa: BLE001
                out[(schema_name, table)] = []  # signals "drop" to caller
                dropped.append(f"{schema_name}.{table}")
                self.logger.warning(
                    "Column reflection failed for `%s.%s`: %s", schema_name, table, e,
                )
        return out

    def _safe_multi_indexes(self, inspected, schema_name: str, index_skipped: list) -> dict:
        """Bulk index reflection; per-table fallback. A table that fails
        per-table keeps an empty index list (table is retained)."""
        try:
            return inspected.get_multi_indexes(schema=schema_name)
        except Exception as e:  # noqa: BLE001
            self.logger.warning(
                "Bulk index reflection failed for schema `%s`; falling back to "
                "per-table. Error: %s", schema_name, e,
            )
        out: dict = {}
        try:
            names = inspected.get_table_names(schema=schema_name)
        except Exception:  # noqa: BLE001
            names = []
        for table in names:
            try:
                out[(schema_name, table)] = inspected.get_indexes(table, schema=schema_name)
            except Exception as e:  # noqa: BLE001
                out[(schema_name, table)] = []  # optional -> keep table, no indices
                index_skipped.append(f"{schema_name}.{table}")
                self.logger.warning(
                    "Index reflection failed for `%s.%s`: %s", schema_name, table, e,
                )
        return out

    # ---------- A: primary keys from information_schema ----------
    def _fetch_primary_keys(self) -> dict:
        """A — one bulk query for all PKs in the catalog via information_schema.

        Best-effort: a failure here logs a warning and leaves keys empty rather
        than breaking discovery.
        """
        catalog = self.config["catalog"]
        query = text(f"""
            SELECT kcu.table_schema, kcu.table_name, kcu.column_name
            FROM `{catalog}`.information_schema.table_constraints tc
            JOIN `{catalog}`.information_schema.key_column_usage kcu
              ON tc.constraint_catalog = kcu.constraint_catalog
             AND tc.constraint_schema  = kcu.constraint_schema
             AND tc.constraint_name    = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position
        """)
        pk_map: dict = {}
        try:
            with self.create_engine().connect() as conn:
                for schema_name, table_name, column_name in conn.execute(query):
                    pk_map.setdefault((schema_name, table_name), []).append(column_name)
        except Exception as e:  # noqa: BLE001 - best-effort enrichment
            self.logger.warning("Could not fetch primary keys: %s", e)
        return pk_map

    # ---------- A + B: enrich one entry ----------
    def _enrich_entry(self, entry: dict, pk_map: dict) -> None:
        # locate the stream-level ([]) metadata block
        stream_md = next(
            (m["metadata"] for m in entry.get("metadata", []) if m.get("breadcrumb") == []),
            None,
        )
        if stream_md is None:
            return
        schema_name = stream_md.get("schema-name")
        table_name = stream_md.get("table-name")
        props = entry.get("schema", {}).get("properties", {})
        lower_props = {k.lower(): k for k in props}  # map for case-correct lookups

        # A — inject PKs only if none already present (never clobber user/dialect)
        pk = pk_map.get((schema_name, table_name))
        if pk and not entry.get("key_properties"):
            pk_cased = [lower_props.get(c.lower(), c) for c in pk]
            entry["key_properties"] = pk_cased
            stream_md["table-key-properties"] = pk_cased

        # B — suggest a replication key only if none already set (suggest-only:
        # set the key, leave replication-method as discovered/full_table)
        if not entry.get("replication_key"):
            for cand in REPLICATION_KEY_CANDIDATES:
                if cand in lower_props:
                    col = lower_props[cand]
                    entry["replication_key"] = col
                    stream_md["replication-key"] = col
                    break
        
        # C adding replication method and selected key for safety
        # stream_md["replication-method"] = entry["replication_method"]
        # stream_md["selected"] = True


class DatabricksStream(SQLStream):
    """Stream class for Databricks streams.

    This class handles stream-specific logic including record retrieval,
    query filtering, and data processing. Developers may override methods
    to customize stream behavior.
    """

    connector_class = DatabricksConnector
    # ABORT_AT_RECORD_COUNT = 1   # forces a server-side LIMIT on every query reagardless run as sync or test

    # Query and data processing configuration
    supports_nulls_first = True  # Whether the database supports NULLS FIRST/LAST

    # Type conformance level - controls how strictly data types are enforced
    # ROOT_ONLY: Only enforce types at the root level (useful for JSON/JSONB columns)
    # RECURSIVE: Recursively enforce types throughout nested structures
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.RECURSIVE

    @override
    def apply_query_filters(
        self,
        query: sqlalchemy.sql.Select,
        table: sqlalchemy.Table,
        *,
        context: Context | None = None,
    ) -> sqlalchemy.sql.Select:
        """Apply custom query filters to the SELECT query.

        Developers may override this method to add custom WHERE clauses,
        JOIN operations, or other query modifications based on configuration
        or stream-specific requirements.

        Args:
            query: The base SELECT query
            table: The SQLAlchemy table object
            context: Stream partition or context dictionary

        Returns:
            Modified SELECT query
        """
        query = super().apply_query_filters(query, table, context=context)

        # Add custom WHERE clauses from configuration, etc.
        # query = query.where(...)

        return query  # noqa: RET504

    @override
    def get_records(self, context: Context | None) -> Iterable[Record]:
        """Return a generator of record-type dictionary objects.

        Developers may override this method to implement custom record retrieval
        logic, such as batch processing, result set optimization, or custom
        data transformations. This is particularly useful when the source database
        provides batch-optimized record retrieval mechanisms.

        Args:
            context: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Example implementation with query optimization:
        # 1. Get selected columns to avoid SELECT *
        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        # 2. Apply replication key ordering and filtering
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.where(replication_key_col >= start_val)

        # 3. Execute query and yield records
        with self.connector._connect() as connection:  # noqa: SLF001
            for record in connection.execute(query).mappings():
                yield dict(record)

        # Alternative: Use the default implementation
        # yield from super().get_records(context)