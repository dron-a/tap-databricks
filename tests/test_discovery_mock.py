"""Mocked unit tests — run offline, no Databricks connection.

These form the public-CI backbone and cover the logic:
  A  primary-key injection (_enrich_entry)
  B  replication-key auto-detect (_enrich_entry, suggest-only)
  E  resilient reflection fallbacks (_safe_multi_columns / _safe_multi_indexes / _safe_multi_pk)
  auth path selection (create_engine: PAT-first, SP fallback, error)

They exercise the helpers directly at the SQLAlchemy-inspector level, so no
real connection or credentials are required.
"""
import pytest

from tap_databricks.client import DatabricksConnector, REPLICATION_KEY_CANDIDATES

PAT_CONFIG = {
    "server_hostname": "example.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/abc",
    "catalog": "cat",
    "access_token": "dapi-fake",
}
SP_CONFIG = {
    "server_hostname": "example.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/abc",
    "catalog": "cat",
    "client_id": "cid",
    "client_secret": "csecret",
}
NO_AUTH_CONFIG = {
    "server_hostname": "example.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/abc",
    "catalog": "cat",
}


@pytest.fixture
def connector():
    """A connector instance for helper tests (auth irrelevant here)."""
    return DatabricksConnector(config=dict(PAT_CONFIG))


def _entry(props, schema="s", table="t", **top):
    """Build a minimal catalog-entry dict shaped like the SDK's to_dict()."""
    e = {
        "schema": {"properties": {p: {"type": ["string"]} for p in props}},
        "metadata": [
            {"breadcrumb": [], "metadata": {"schema-name": schema, "table-name": table}}
        ],
    }
    e.update(top)
    return e


# ---------------- A: primary-key injection ----------------

def test_a_injects_pk_case_matched(connector):
    entry = _entry(["Id", "LASTMODIFIEDDATE"])
    connector._enrich_entry(entry, {("s", "t"): ["id"]})  # info_schema lower-case
    # case-matched to the actual property name
    assert entry["key_properties"] == ["Id"]
    md = entry["metadata"][0]["metadata"]
    assert md["table-key-properties"] == ["Id"]


def test_a_composite_pk(connector):
    entry = _entry(["Id", "SnapshotDate"])
    connector._enrich_entry(entry, {("s", "t"): ["id", "snapshotdate"]})
    assert entry["key_properties"] == ["Id", "SnapshotDate"]


def test_a_no_clobber_existing_key(connector):
    entry = _entry(["Id"], key_properties=["Existing"])
    connector._enrich_entry(entry, {("s", "t"): ["id"]})
    assert entry["key_properties"] == ["Existing"]  # untouched


def test_a_empty_when_no_pk(connector):
    entry = _entry(["Id"])
    connector._enrich_entry(entry, {})  # no PK for this table
    assert "key_properties" not in entry or entry.get("key_properties") in (None, [])


# ---------------- B: replication-key auto-detect ----------------

def test_b_detects_candidate(connector):
    entry = _entry(["Id", "LASTMODIFIEDDATE"])
    connector._enrich_entry(entry, {})
    assert entry["replication_key"] == "LASTMODIFIEDDATE"
    assert entry["metadata"][0]["metadata"]["replication-key"] == "LASTMODIFIEDDATE"


def test_b_suggest_only_no_method(connector):
    entry = _entry(["Id", "updated_at"])
    connector._enrich_entry(entry, {})
    assert entry["replication_key"] == "updated_at"
    # suggest-only: method must NOT be set by our code
    assert "replication_method" not in entry
    assert "replication-method" not in entry["metadata"][0]["metadata"]


def test_b_priority_order(connector):
    # both present; first candidate in the list wins
    entry = _entry(["updated_at", "lastmodifieddate"])
    connector._enrich_entry(entry, {})
    assert entry["replication_key"].lower() == REPLICATION_KEY_CANDIDATES[0]


def test_b_no_clobber_existing(connector):
    entry = _entry(["updated_at"], replication_key="Chosen")
    connector._enrich_entry(entry, {})
    assert entry["replication_key"] == "Chosen"


def test_b_no_candidate(connector):
    entry = _entry(["colA", "colB"])
    connector._enrich_entry(entry, {})
    assert "replication_key" not in entry


# ---------------- E: resilient reflection ----------------

class FakeInspector:
    def __init__(self, *, multi_columns=None, multi_indexes=None, multi_pk=None,
                 raise_columns=False, raise_indexes=False, raise_pk=False,
                 table_names=(), per_table_columns=None, per_table_indexes=None):
        self._mc, self._mi, self._mpk = multi_columns, multi_indexes, multi_pk
        self._rc, self._ri, self._rpk = raise_columns, raise_indexes, raise_pk
        self._tn = list(table_names)
        self._ptc = per_table_columns or {}
        self._pti = per_table_indexes or {}

    def get_multi_columns(self, schema, kind):
        if self._rc:
            raise RuntimeError("bulk columns boom")
        return self._mc

    def get_multi_indexes(self, schema):
        if self._ri:
            raise RuntimeError("bulk indexes boom")
        return self._mi

    def get_multi_pk_constraint(self, schema):
        if self._rpk:
            raise RuntimeError("bulk pk boom")
        return self._mpk

    def get_table_names(self, schema):
        return self._tn

    def get_columns(self, table, schema):
        v = self._ptc[table]
        if isinstance(v, Exception):
            raise v
        return v

    def get_indexes(self, table, schema):
        v = self._pti[table]
        if isinstance(v, Exception):
            raise v
        return v


def test_e_columns_bulk_success(connector):
    insp = FakeInspector(multi_columns={("s", "t"): [{"name": "c"}]})
    dropped = []
    out = connector._safe_multi_columns(insp, "s", object_kind=None, dropped=dropped)
    assert out == {("s", "t"): [{"name": "c"}]}
    assert dropped == []


def test_e_columns_fallback_drops_bad_table(connector):
    insp = FakeInspector(
        raise_columns=True,
        table_names=["good", "bad"],
        per_table_columns={"good": [{"name": "c"}], "bad": RuntimeError("nope")},
    )
    dropped = []
    out = connector._safe_multi_columns(insp, "s", object_kind=None, dropped=dropped)
    assert out[("s", "good")] == [{"name": "c"}]
    assert out[("s", "bad")] == []       # empty -> caller drops it
    assert "s.bad" in dropped


def test_e_indexes_fallback_retains_bad_table(connector):
    insp = FakeInspector(
        raise_indexes=True,
        table_names=["good", "bad"],
        per_table_indexes={"good": [{"name": "i"}], "bad": RuntimeError("nope")},
    )
    skipped = []
    out = connector._safe_multi_indexes(insp, "s", index_skipped=skipped)
    assert out[("s", "good")] == [{"name": "i"}]
    assert out[("s", "bad")] == []       # empty -> table retained, no indices
    assert "s.bad" in skipped


def test_e_pk_bulk_failure_returns_empty_no_fallback(connector):
    insp = FakeInspector(raise_pk=True)
    # no fallback: just returns {} (A sources keys from information_schema)
    assert connector._safe_multi_pk(insp, "s") == {}


# ---------------- auth path selection ----------------

def test_auth_pat_first(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        "sqlalchemy.create_engine",
        lambda url, **kw: captured.update(url=url, kw=kw) or "ENGINE",
    )
    DatabricksConnector(config=dict(PAT_CONFIG)).create_engine()
    assert "connect_args" not in captured["kw"]   # PAT rides in URL


def test_auth_sp_uses_credentials_provider(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        "sqlalchemy.create_engine",
        lambda url, **kw: captured.update(url=url, kw=kw) or "ENGINE",
    )
    DatabricksConnector(config=dict(SP_CONFIG)).create_engine()
    assert "credentials_provider" in captured["kw"]["connect_args"]


def test_auth_none_raises(monkeypatch):
    monkeypatch.setattr("sqlalchemy.create_engine", lambda *a, **k: "ENGINE")
    with pytest.raises(ValueError):
        DatabricksConnector(config=dict(NO_AUTH_CONFIG)).create_engine()