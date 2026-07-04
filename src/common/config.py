"""Configuration loading, table naming, and the run-tag marker.

The run-tag marker is the linchpin of the "prove it from the platform" design:
every benchmark query is prefixed with a SQL comment like

    /* momos_bench run=<run_tag> tpl=q07_top_products */ SELECT ...

Because ``system.query.history.statement_text`` preserves that comment, we can
filter the platform's own audit log down to *exactly* the queries of one run
and COUNT / percentile them authoritatively — no reliance on client self-report.
"""
from __future__ import annotations

import datetime as _dt
import os
from typing import Any, Optional

import yaml

_DEFAULT_CONFIG = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
)


def load_config(path: Optional[str] = None) -> dict[str, Any]:
    """Load config.yaml. Search order: explicit arg -> $MOMOS_CONFIG -> packaged default."""
    path = path or os.environ.get("MOMOS_CONFIG") or _DEFAULT_CONFIG
    with open(path, "r") as fh:
        return yaml.safe_load(fh)


# --- table / schema naming ------------------------------------------------

def schema_fqn(cfg: dict) -> str:
    return f"{cfg['databricks']['catalog']}.{cfg['databricks']['schema']}"


def fq(cfg: dict, table_key: str) -> str:
    """Fully-qualified name for a logical table key from cfg['tables']."""
    return f"{schema_fqn(cfg)}.{cfg['tables'][table_key]}"


def get_scale(cfg: dict, name: Optional[str] = None) -> dict[str, int]:
    """Resolve a scale profile (row counts). Falls back to cfg['active_scale']."""
    name = name or cfg.get("active_scale", "sf1")
    if name not in cfg["scale_profiles"]:
        raise KeyError(f"Unknown scale profile '{name}'. "
                       f"Known: {list(cfg['scale_profiles'])}")
    return cfg["scale_profiles"][name]


# --- run tagging ----------------------------------------------------------

def make_run_tag(prefix: str, mode: str, scale: str) -> str:
    """Stable, human-readable, unique-per-run identifier."""
    ts = _dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}_{mode}_{scale}"


def run_marker(run_tag: str, template_id: str, nonce=None) -> str:
    """SQL comment prepended to every benchmark query. Kept on one line so it
    survives intact in system.query.history.statement_text.

    ``nonce`` (compute mode) makes each statement's text unique, which busts the
    result cache without needing a session-level SET — so it works with the
    stateless Statement Execution API. Omit it (serving mode) for identical text
    that the result cache can serve. The run-tag prefix and ``tpl=`` token are
    unaffected, so history filtering and per-template extraction still work."""
    if nonce is None:
        return f"/* {run_tag} tpl={template_id} */"
    return f"/* {run_tag} tpl={template_id} n={nonce} */"


def history_like_pattern(run_tag: str) -> str:
    """LIKE pattern to isolate one run's queries in system.query.history.

    Matches statements that *begin* with the run marker (``/* <run_tag> tpl=... */``),
    so the analysis queries themselves — which merely contain <run_tag> as a
    literal inside a WHERE clause and start with SELECT — never self-count."""
    return f"/* {run_tag} %"
