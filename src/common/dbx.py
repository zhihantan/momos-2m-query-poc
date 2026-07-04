"""Robust connection resolution for the SQL connector from inside Databricks
compute (works on serverless, where ``spark.conf`` may not carry the workspace
URL). Includes a time-boxed preflight so a bad host/token fails fast with a clear
message instead of hanging the whole load generator.
"""
from __future__ import annotations

import threading
from typing import Optional


def _clean_host(h: str) -> str:
    return h.replace("https://", "").replace("http://", "").rstrip("/")


def resolve_host(spark=None) -> str:
    if spark is not None:
        try:
            h = spark.conf.get("spark.databricks.workspaceUrl")
            if h:
                return _clean_host(h)
        except Exception:  # noqa: BLE001 — serverless may not expose this conf
            pass
    from databricks.sdk import WorkspaceClient
    return _clean_host(WorkspaceClient().config.host)


def resolve_token(dbutils=None) -> Optional[str]:
    # 1) the notebook run's scoped API token (works on classic and serverless)
    if dbutils is not None:
        try:
            t = (dbutils.notebook.entry_point.getDbutils().notebook()
                 .getContext().apiToken().get())
            if t:
                return t
        except Exception:  # noqa: BLE001
            pass
    # 2) a static token from the SDK config (None when auth is OAuth)
    try:
        from databricks.sdk import WorkspaceClient
        t = WorkspaceClient().config.token
        if t:
            return t
    except Exception:  # noqa: BLE001
        pass
    return None


def http_path(warehouse_id: str) -> str:
    return f"/sql/1.0/warehouses/{warehouse_id}"


def open_connection(host: str, http_path_: str, token: str):
    """Open one SQL-connector connection to the warehouse."""
    from databricks import sql
    return sql.connect(server_hostname=host, http_path=http_path_, access_token=token)


def preflight(host: str, http_path_: str, token: Optional[str], timeout_s: int = 30) -> None:
    """One connect + SELECT 1, time-boxed. Raises a clear error on failure/hang so
    we never launch a fleet of threads against an unreachable endpoint."""
    if not host:
        raise RuntimeError("Could not resolve workspace host (server_hostname is empty).")
    if not token:
        raise RuntimeError("Could not resolve an access token for the SQL connector.")
    box: dict = {}

    def _try():
        try:
            conn = open_connection(host, http_path_, token)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            cur.close(); conn.close()
            box["ok"] = True
        except Exception as e:  # noqa: BLE001
            box["err"] = repr(e)[:400]

    th = threading.Thread(target=_try, daemon=True)
    th.start()
    th.join(timeout_s)
    if th.is_alive():
        raise RuntimeError(
            f"Preflight connect HUNG >{timeout_s}s to {host}{http_path_}. "
            f"Usually a wrong server_hostname or an unreachable warehouse.")
    if not box.get("ok"):
        raise RuntimeError(f"Preflight connect FAILED to {host}{http_path_}: {box.get('err')}")
