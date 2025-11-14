#!/usr/bin/env python3
"""
CSV/Excel -> Supabase upsert utility (with .env support and SUPABASE_URL validation)

Usage:
  - Create a .env file with SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY
  - Run: python import_banks.py --csv banks.csv --dry-run

Dependencies:
  pip install pandas rapidfuzz supabase python-dotenv
  # Optional for HTTP connectivity check:
  pip install requests
#SUPABASE_URL="https://emxjlyigrdmgiebevyzx.supabase.co"
#SUPABASE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVteGpseWlncmRtZ2llYmV2eXp4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MDE2ODU0NSwiZXhwIjoyMDc1NzQ0NTQ1fQ.udG69-5tFqYAyh0iCHc7WGmQADrcl-2P9jsROOat-to"

"""

from __future__ import annotations
import argparse
import csv
import json
import os
import re
import socket
import sys
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
from pandas.errors import ParserError

# load .env early
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

try:
    from rapidfuzz import process, fuzz
except Exception:
    raise ImportError("Please install rapidfuzz (pip install rapidfuzz)")

try:
    from supabase import create_client
except Exception:
    raise ImportError("Please install supabase (pip install supabase)")

# Optional requests-based check
try:
    import requests  # type: ignore
    HAS_REQUESTS = True
except Exception:
    HAS_REQUESTS = False

# ---------- URL validation ----------

def validate_supabase_url(supabase_url: Optional[str]) -> Tuple[str, str]:
    """
    Validate format of SUPABASE_URL and return (clean_url, hostname).
    Raises RuntimeError with actionable message on failure.
    """
    if not supabase_url:
        raise RuntimeError("SUPABASE_URL is missing. Set SUPABASE_URL in environment or pass --supabase-url.")
    supabase_url = supabase_url.strip()
    parsed = urlparse(supabase_url)
    if parsed.scheme not in ("https", "http"):
        raise RuntimeError(f"SUPABASE_URL must start with https:// (got: {supabase_url}). Use the full URL from your Supabase project.")
    if not parsed.hostname:
        raise RuntimeError(f"Could not parse hostname from SUPABASE_URL: {supabase_url}")
    # Basic sanity: host should contain "supabase" or end with ".supabase.co" often
    # but many self-hosted setups exist so we don't enforce that strongly.
    return supabase_url, parsed.hostname

def resolve_hostname(hostname: str, timeout: float = 5.0) -> None:
    """
    Attempt to resolve the hostname. Raises RuntimeError on failure with hints.
    """
    try:
        # set default timeout for socket operations
        orig_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(timeout)
        infos = socket.getaddrinfo(hostname, None)
        socket.setdefaulttimeout(orig_timeout)
        if not infos:
            raise RuntimeError(f"No address info found for {hostname}")
    except Exception as e:
        raise RuntimeError(
            f"DNS resolution failed for hostname '{hostname}': {e}\n"
            "Hints: check SUPABASE_URL for typos, ensure network/DNS is working, try 'nslookup' or 'ping' on your machine, or switch networks (home/mobile hotspot)."
        ) from e

def optional_http_head_check(url: str, timeout: float = 5.0) -> None:
    """
    Optional lightweight HTTP HEAD request to confirm reachability.
    Requires 'requests' package; skips if not available.
    """
    if not HAS_REQUESTS:
        return
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True)
        # Accept 2xx and 3xx as reachable. 4xx/5xx still indicates host reachable but endpoint may be restricted.
        if resp.status_code >= 500:
            raise RuntimeError(f"HTTP HEAD to {url} returned status {resp.status_code}")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"HTTP connectivity check to {url} failed: {e}")

# ---------- (rest of the script unchanged except we invoke validation before upsert) ----------

def parse_db_columns(s: str) -> List[str]:
    return [c.strip() for c in s.split(",") if c.strip()]

def load_mapping(mapping_file: Optional[str]) -> Optional[Dict[str, str]]:
    if not mapping_file:
        return None
    if not os.path.exists(mapping_file):
        return None
    try:
        with open(mapping_file, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in mapping file {mapping_file}: {e}")

def _normalize_name(s: str) -> str:
    s2 = (s or "").lower().strip()
    s2 = re.sub(r"[^\w]", " ", s2)
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2

def detect_mapping_orientation(mapping: Dict[str, str], db_cols: List[str]) -> Dict[str, str]:
    if not mapping:
        return {}
    db_set = set(db_cols)
    key_hits = sum(1 for k in mapping.keys() if k in db_set)
    val_hits = sum(1 for v in mapping.values() if v in db_set)
    if key_hits > val_hits:
        return {v: k for k, v in mapping.items()}
    return mapping

def fuzzy_map_columns(df_cols: List[str], db_cols: List[str], threshold: float) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    remaining_src = set(df_cols)
    score_cutoff = int(max(0, min(1.0, threshold)) * 100)
    src_norm_map = {s: _normalize_name(s) for s in remaining_src}
    for db_col in db_cols:
        if not remaining_src:
            break
        db_norm = _normalize_name(db_col)
        choices = list(remaining_src)
        best = process.extractOne(
            db_norm,
            {s: src_norm_map[s] for s in choices},
            scorer=fuzz.token_sort_ratio,
            processor=None,
        )
        if best:
            matched_src = best[2]
            score = best[1]
            if score >= score_cutoff:
                mapping[matched_src] = db_col
                remaining_src.remove(matched_src)
    return mapping

def robust_read(path: str, encoding: str = "utf-8", try_detect_delimiter: bool = True) -> pd.DataFrame:
    path_l = path.lower()
    if path_l.endswith((".xls", ".xlsx")):
        return pd.read_excel(path)

    detected_sep = None
    if try_detect_delimiter:
        try:
            with open(path, "r", encoding=encoding, errors="replace", newline="") as fh:
                sample = fh.read(8192)
                fh.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                    detected_sep = dialect.delimiter
                except Exception:
                    detected_sep = None
        except FileNotFoundError:
            raise

    read_attempts = []
    if detected_sep:
        read_attempts.append({"sep": detected_sep, "engine": "c"})
    read_attempts.extend([
        {"sep": ",", "engine": "c"},
        {"sep": ";", "engine": "c"},
        {"sep": ",", "engine": "python"},
        {"sep": None, "engine": "python"},
    ])

    last_err = None
    for opts in read_attempts:
        try:
            df = pd.read_csv(path, sep=opts["sep"], engine=opts["engine"], encoding=encoding)
            return df
        except ParserError as e:
            last_err = e
        except Exception as e:
            last_err = e

    try:
        df = pd.read_csv(path, engine="python", encoding=encoding, on_bad_lines="warn")
        return df
    except Exception:
        print("Failed to parse CSV. First 60 lines for manual inspection:", file=sys.stderr)
        with open(path, "r", encoding=encoding, errors="replace") as fh:
            for i, line in enumerate(fh):
                if i >= 60:
                    break
                print(f"{i+1:03d}: {line.rstrip()}", file=sys.stderr)
        raise last_err

import numpy as np

def _to_python_value(v: Any) -> Any:
    if v is None:
        return None
    if v is pd.NA:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, (np.integer, )):
        return int(v)
    if isinstance(v, (np.floating, )):
        return float(v)
    if isinstance(v, (np.bool_, )):
        return bool(v)
    if isinstance(v, Decimal):
        return float(v)
    if hasattr(v, "isoformat") and not isinstance(v, str):
        try:
            return v.isoformat()
        except Exception:
            pass
    if isinstance(v, (np.str_,)):
        return str(v)
    return v

def records_to_python(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in records:
        nr = {k: _to_python_value(v) for k, v in r.items()}
        out.append(nr)
    return out

def normalize_and_prepare_records(
    df: pd.DataFrame,
    db_cols: List[str],
    mapping_src_to_db: Dict[str, str],
    coercions: Optional[Dict[str, str]] = None,
    ensure_created_at: bool = True,
) -> List[Dict[str, Any]]:
    if coercions is None:
        coercions = {}

    df_renamed = df.rename(columns=mapping_src_to_db)

    if ensure_created_at and "created_at" not in df_renamed.columns:
        df_renamed["created_at"] = pd.Timestamp.utcnow()

    for c in db_cols:
        if c not in df_renamed.columns:
            df_renamed[c] = None

    for col, typ in coercions.items():
        if col not in df_renamed.columns:
            continue
        if typ == "date":
            df_renamed[col] = pd.to_datetime(df_renamed[col], errors="coerce").dt.date
        elif typ in ("datetime", "timestamp"):
            df_renamed[col] = pd.to_datetime(df_renamed[col], errors="coerce")
        elif typ in ("int", "integer"):
            df_renamed[col] = pd.to_numeric(df_renamed[col], errors="coerce").astype("Int64")
        elif typ in ("float", "numeric"):
            df_renamed[col] = pd.to_numeric(df_renamed[col], errors="coerce")

    df_final = df_renamed[db_cols]
    df_final = df_final.where(pd.notnull(df_final), None)

    for col in df_final.columns:
        col_series = df_final[col]
        if pd.api.types.is_datetime64_any_dtype(col_series.dtype) or pd.api.types.is_datetime64tz_dtype(col_series.dtype):
            df_final[col] = col_series.apply(lambda x: x.isoformat() if x is not None else None)
        else:
            sample_vals = col_series.dropna().head(10).tolist()
            if any(hasattr(v, "isoformat") and not isinstance(v, str) for v in sample_vals):
                df_final[col] = col_series.apply(lambda x: x.isoformat() if (x is not None and hasattr(x, "isoformat") and not isinstance(x, str)) else x)

    records = df_final.to_dict(orient="records")
    records = records_to_python(records)
    return records

def chunked_iterable(it: Iterable, size: int) -> Iterable[List]:
    buf = []
    for item in it:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def _mask_key(k: Optional[str]) -> str:
    if not k:
        return "<none>"
    return k[:6] + "..." + k[-4:]

def upsert_to_supabase(
    records: List[Dict[str, Any]],
    table_name: str,
    on_conflict: str,
    supabase_url: Optional[str],
    supabase_key: Optional[str],
    batch_size: int = 500,
    verbose: bool = True,
    http_check: bool = False,
) -> List[Tuple[bool, Any]]:
    if not supabase_url:
        supabase_url = os.environ.get("SUPABASE_URL")
    if not supabase_key:
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("Supabase URL and KEY must be provided via args or environment variables. For server-side writes prefer SUPABASE_SERVICE_ROLE_KEY.")

    # validate URL and resolve hostname
    clean_url, hostname = validate_supabase_url(supabase_url)
    try:
        resolve_hostname(hostname)
    except RuntimeError as e:
        raise

    if http_check:
        try:
            optional_http_head_check(clean_url)
        except RuntimeError as e:
            # not fatal; show warning and continue (user can disable http_check)
            print(f"Warning: HTTP check failed: {e}", file=sys.stderr)

    # masked debug print
    print(f"Using SUPABASE_URL={clean_url}")
    if os.environ.get("SUPABASE_SERVICE_ROLE_KEY"):
        print("Using SUPABASE_SERVICE_ROLE_KEY (masked):", _mask_key(os.environ.get("SUPABASE_SERVICE_ROLE_KEY")))
    elif os.environ.get("SUPABASE_KEY"):
        print("Using SUPABASE_KEY (masked):", _mask_key(os.environ.get("SUPABASE_KEY")))
    else:
        print("Using key from CLI argument (masked):", _mask_key(supabase_key))

    if os.environ.get("SUPABASE_KEY") and not os.environ.get("SUPABASE_SERVICE_ROLE_KEY"):
        print("WARNING: SUPABASE_KEY (anon) is being used. This may fail if RLS blocks writes. Prefer SUPABASE_SERVICE_ROLE_KEY for server scripts.", file=sys.stderr)

    client = create_client(clean_url, supabase_key)
    results = []
    for i, batch in enumerate(chunked_iterable(records, batch_size)):
        try:
            if verbose:
                print(f"Upserting batch {i+1} (size {len(batch)})...")
            res = client.table(table_name).upsert(batch, on_conflict=on_conflict).execute()
            failed = False
            err = None
            if hasattr(res, "error") and res.error:
                failed = True
                err = res.error
            elif isinstance(res, dict) and res.get("error"):
                failed = True
                err = res.get("error")
            if failed:
                print(f"Batch {i+1} upsert error: {err}", file=sys.stderr)
                results.append((False, err))
            else:
                results.append((True, res))
        except Exception as e:
            print(f"Upsert batch {i+1} failed: {e}", file=sys.stderr)
            results.append((False, e))
    return results

def load_and_upsert(
    csv_path: str,
    table_name: str,
    db_cols: List[str],
    mapping_file: Optional[str],
    dry_run: bool,
    fuzzy_threshold: float,
    on_conflict: str,
    supabase_url: Optional[str],
    supabase_key: Optional[str],
    batch_size: int = 500,
    encoding: str = "utf-8",
    try_detect_delimiter: bool = True,
    coercions: Optional[Dict[str, str]] = None,
    http_check: bool = False,
):
    df = robust_read(csv_path, encoding=encoding, try_detect_delimiter=try_detect_delimiter)

    original_cols = list(df.columns)

    mapping_src_to_db: Dict[str, str] = {}
    manual_mapping = load_mapping(mapping_file)
    if manual_mapping:
        mapping_src_to_db = detect_mapping_orientation(manual_mapping, db_cols)
    else:
        mapping_src_to_db = fuzzy_map_columns(original_cols, db_cols, fuzzy_threshold)

    print("Column mapping (source -> db):")
    for src, tgt in mapping_src_to_db.items():
        print(f"  '{src}' -> '{tgt}'")

    unmapped_db_cols = [c for c in db_cols if c not in mapping_src_to_db.values()]
    if unmapped_db_cols:
        print(f"DB columns without matched source column: {unmapped_db_cols}", file=sys.stderr)

    if coercions is None:
        coercions = {
            "bank_id": "int",
            "created_at": "datetime",
        }

    records = normalize_and_prepare_records(
        df, db_cols, mapping_src_to_db, coercions=coercions, ensure_created_at=True
    )

    print(f"Prepared {len(records)} records (showing first 5):")
    for rec in records[:5]:
        print(rec)

    if dry_run:
        print("Dry-run enabled; no data uploaded to Supabase.")
        return

    upsert_results = upsert_to_supabase(
        records,
        table_name,
        on_conflict,
        supabase_url,
        supabase_key,
        batch_size=batch_size,
        http_check=http_check,
    )

    success_count = sum(1 for ok, _ in upsert_results if ok)
    fail_count = sum(1 for ok, _ in upsert_results if not ok)
    print(f"Upsert completed. Successful batches: {success_count}, Failed: {fail_count}")
    if fail_count:
        print("See stderr for batch errors.")

def main():
    p = argparse.ArgumentParser(description="Load CSV/Excel and upsert to Supabase table")
    p.add_argument("--csv", default="banks.xlsx", help="Path to CSV/Excel file")
    p.add_argument("--table", default="banks", help="Target Supabase table name")
    p.add_argument(
        "--db-columns",
        default="bank_id,name,bic,country,created_at,fd_mature_status",
        help="Comma-separated DB column list (in preferred order)",
    )
    p.add_argument("--mapping-file", default=None, help="Optional manual mapping JSON file")
    p.add_argument("--dry-run", action="store_true", help="Do not write to Supabase; only show prepared records")
    p.add_argument("--threshold", type=float, default=0.7, help="Fuzzy match threshold (0-1)")
    p.add_argument("--on-conflict", default="bank_id", help="Column name to use for upsert conflict resolution")
    p.add_argument("--supabase-url", default=None, help="Supabase URL (overrides env)")
    p.add_argument("--supabase-key", default=None, help="Supabase key (overrides env). Prefer service role key.")
    p.add_argument("--batch-size", type=int, default=500, help="Upsert batch size")
    p.add_argument("--encoding", default="utf-8", help="File encoding for CSV files")
    p.add_argument("--no-detect-delimiter", dest="detect_delimiter", action="store_false", help="Disable auto delimiter detection for CSV")
    p.add_argument("--http-check", action="store_true", help="Perform an optional HTTP HEAD check against SUPABASE_URL (requires requests)")
    args = p.parse_args()

    db_columns = parse_db_columns(args.db_columns)
    load_and_upsert(
        csv_path=args.csv,
        table_name=args.table,
        db_cols=db_columns,
        mapping_file=args.mapping_file,
        dry_run=args.dry_run,
        fuzzy_threshold=args.threshold,
        on_conflict=args.on_conflict,
        supabase_url=args.supabase_url,
        supabase_key=args.supabase_key,
        batch_size=args.batch_size,
        encoding=args.encoding,
        try_detect_delimiter=args.detect_delimiter,
        http_check=args.http_check,
    )

if __name__ == "__main__":

    main()
