#!/usr/bin/env python3
"""
EDM_Signatory_General_Check.py
==============================

What this script does
---------------------
For every current MP (or Peer), this script checks whether they have signed
any Early Day Motion (EDM) tabled in a chosen date window, using the UK
Parliament open APIs. It produces two CSV files:

  * edm_sponsors.csv         -- one row per EDM. This is a master cache: on
                                each run, only *new* EDMs are fetched from
                                the API and appended. Delete the file if you
                                want to rebuild it from scratch.

  * edm_signing_status<..>.csv -- one row per MP, answering "did this MP
                                sign any matching EDM?". The filename
                                includes a run label so filtered runs don't
                                overwrite each other.

Who this is for
---------------
You do NOT need to know Python to use this script. Open it in any text
editor (e.g. Notepad, VS Code), edit the CONFIG block below, then run:

    python EDM_Signatory_General_Check.py

from a terminal opened in this folder.

See README.md in this folder for setup (installing Python, installing the
required libraries) and examples of common configurations.

Typical run times
-----------------
  * First run ever:         ~30-60 minutes (fetches ~3,000+ EDMs one by one)
  * Subsequent runs:        ~1-5 minutes (only new EDMs are fetched)
  * After changing filters: a few seconds (no network needed if master is
                                           already up to date)

Data source
-----------
  * Members API:             https://members-api.parliament.uk
  * Oral Questions & Motions API:
                             https://oralquestionsandmotions-api.parliament.uk
Open Parliament Licence. No API key or login required.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import requests

# The script lives in <project>/src/. This is the project root -- every
# relative path in the CONFIG block below is resolved against it, so the
# script works whether you run it from the Code folder or from inside src/.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Captured once at script start so every output file from a given run shares
# the same date+time stamp (the CSV and XLSX stay in sync).
_RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


# =============================================================================
# CONFIG -- edit the values in this block to change what the script audits.
# Everything below the "END OF CONFIG" line is internal logic; you should not
# need to touch it.
# =============================================================================

# --- Date window ----------------------------------------------------------
# Only EDMs tabled within this window are counted. Format: "YYYY-MM-DD".
# Set END_DATE to None to mean "up to today".
#
# Note: the master edm_sponsors.csv accumulates over time. If you widen the
# date window to reach further BACK than your existing CSV covers, delete
# edm_sponsors.csv first so the script rebuilds from the new start date.
START_DATE = "2024-07-01"
END_DATE   = "2024-09-11"

# --- Which house to audit -------------------------------------------------
# "commons" = current MPs (the usual case -- EDMs are a Commons device).
# "lords"   = current Peers. Peers do NOT sign EDMs, so the signing columns
#             will be 0/no for everyone. Only useful if you want a clean
#             list of current Peers for some other purpose.
HOUSE = "commons"

# --- Prayer filter --------------------------------------------------------
# A "prayer" EDM formally prays against a negative statutory instrument;
# they are procedurally different from regular policy-signal EDMs. Choose
# which ones count toward the "signed" tally:
#   "all"              -- count every EDM (default)
#   "exclude_prayers"  -- count only regular (non-prayer) EDMs
#   "only_prayers"     -- count only prayer EDMs
PRAYER_MODE = "all"

# --- Party filter ---------------------------------------------------------
# List of party names to include, or None to include all parties.
# Names are case-insensitive. How they're matched is controlled by
# PARTY_FILTER_MODE below. Examples:
#   ["Labour"]
#   ["Conservative", "Reform UK"]
#   ["Liberal Democrat"]
#   ["Green Party"]
PARTY_FILTER = ["Labour", "Green Party"]

# "substring" (default) -- an MP is included if ANY party in PARTY_FILTER
#   appears as a substring of their party name. Useful because it groups
#   related factions automatically:
#       ["Labour"]  -> matches both "Labour" and "Labour (Co-op)"
#       ["Green"]   -> matches "Green Party"
# "exact" -- an MP is included only if their party name matches one of
#   the strings in PARTY_FILTER verbatim (case-insensitive). Use this if
#   you want to exclude Labour (Co-op) from a pure-Labour run:
#       PARTY_FILTER      = ["Labour"]
#       PARTY_FILTER_MODE = "exact"
#   You can also list variants explicitly:
#       PARTY_FILTER      = ["Labour", "Labour (Co-op)"]
#       PARTY_FILTER_MODE = "exact"
PARTY_FILTER_MODE = "substring"

# --- Constituency filter --------------------------------------------------
# List of constituency names to include, or None to include all.
# Matching is case-insensitive and ignores leading/trailing whitespace.
#   ["Cambridge"]
#   ["Brighton Pavilion", "Bristol Central"]
CONSTITUENCY_FILTER = None

# --- Keyword / theme filter on EDM titles ---------------------------------
# List of keywords -- an EDM is counted only if its title contains at least
# one of these (case-insensitive substring match). None = no keyword filter.
#   ["climate", "net zero", "renewable"]   # climate / energy theme
KEYWORD_FILTER = None

# --- Run label ------------------------------------------------------------
# Affects the output filename so different runs don't overwrite each other.
#   RUN_LABEL = None            -- auto-derive a label from the filters above,
#                                  or use the canonical name when no filters
#                                  are active.
#   RUN_LABEL = "climate_labour" -- produces edm_signing_status_climate_labour.csv
RUN_LABEL = None

# --- Output options -------------------------------------------------------
# Paths below are relative to the project root (the folder that contains
# both "src" and "databases"). Absolute paths are also accepted.
SPONSORS_CSV    = "databases/edm_data/edm_sponsors.csv"     # master EDM data
STATUS_BASENAME = "databases/edm_data/edm_signing_status"   # prefix for run output
WRITE_XLSX      = True                                       # also write .xlsx alongside CSV -- Other option is "False"
CACHE_DIR       = "cache/edm"                                # per-EDM JSON cache (safe to delete)

# =============================================================================
# END OF CONFIG -- do not edit below this line! Unless you want to change how the code operates and not just inputs and outputs.
# =============================================================================


def _abs(p: str) -> str:
    """Resolve a CONFIG path against the project root. Absolute paths pass through."""
    path = Path(p)
    return str(path if path.is_absolute() else _PROJECT_ROOT / path)


# Resolve path-like CONFIG values once, at module load, so the rest of the
# code can treat them as plain absolute strings.
SPONSORS_CSV    = _abs(SPONSORS_CSV)
STATUS_BASENAME = _abs(STATUS_BASENAME)
CACHE_DIR       = _abs(CACHE_DIR)


MEMBERS_API = "https://members-api.parliament.uk/api"
EDMS_API    = "https://oralquestionsandmotions-api.parliament.uk"

USER_AGENT = "EDM-Signing-Audit/1.0 (personal research; contact: you@example.com)"
TIMEOUT    = 30
SLEEP      = 0.15  # polite delay between API calls (seconds)

SPONSORS_HEADER = ["edm_id", "date_tabled", "is_prayer", "title",
                   "tabler_id", "tabler_name", "sponsor_count",
                   "sponsor_ids", "sponsor_names"]

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})


# ---------------------------------------------------------------------------
# Name normalisation
# ---------------------------------------------------------------------------
# Strip honorific prefixes (Mr/Mrs/Ms/Miss/Mx/Dr/Sir/Dame/Lord/Lady/Baroness/
# Rev/Rt Hon/Prof) from MP display names so the status CSV alphabetises by
# actual first name. Handles chained prefixes like "Rt Hon Sir" via re-strip.
HONORIFIC_RE = re.compile(
    r"^(?:"
    r"Rt\s+Hon|Right\s+Hon(?:ourable)?|The\s+Rt\s+Hon|"
    r"Dame|Sir|Lord|Lady|Baroness|Baron|"
    r"Mrs|Miss|Mr|Ms|Mx|"
    r"Dr|Rev|Revd|Reverend|Prof|Professor"
    r")\.?\s+",
    re.IGNORECASE,
)


def strip_honorific(name: str | None) -> str:
    """Remove honorific prefixes from a display name."""
    if not name:
        return ""
    prev = None
    while name != prev:
        prev = name
        name = HONORIFIC_RE.sub("", name, count=1)
    return name.strip()


# ---------------------------------------------------------------------------
# Generic HTTP helper with simple retry/backoff
# ---------------------------------------------------------------------------
def api_get(url: str, params: dict | None = None) -> dict:
    """GET a JSON endpoint, retrying on rate limits or transient errors."""
    for attempt in range(5):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 429:          # rate-limited, back off
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 404:
                return {}
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt == 4:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError("unreachable")


# Case-insensitive field picker -- the two APIs disagree on casing (e.g.
# "Id" vs "id", "Response" vs "response").
def pick(d: dict, *keys, default=None):
    for k in keys:
        if k in d:
            return d[k]
        lk = k[0].lower() + k[1:]
        if lk in d:
            return d[lk]
        uk = k[0].upper() + k[1:]
        if uk in d:
            return d[uk]
    return default


# ---------------------------------------------------------------------------
# Output helpers: CSV (Excel-compatible via utf-8-sig BOM) + optional XLSX
# ---------------------------------------------------------------------------
def _ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def write_csv(path: str, header: list, rows: list) -> None:
    # utf-8-sig writes a BOM so Excel recognises UTF-8 and renders non-ASCII
    # names (e.g. "Sian Berry") correctly.
    _ensure_parent(path)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"Wrote {path}", file=sys.stderr)


def write_xlsx(path: str, sheets: dict) -> None:
    try:
        from openpyxl import Workbook
    except ImportError:
        print(f"  (skipped {path}: install openpyxl to enable -- "
              f"`pip install openpyxl`)", file=sys.stderr)
        return
    _ensure_parent(path)
    wb = Workbook()
    wb.remove(wb.active)
    for name, (header, rows) in sheets.items():
        ws = wb.create_sheet(title=name[:31])  # Excel caps sheet names at 31 chars
        ws.append(header)
        for row in rows:
            ws.append(row)
        ws.freeze_panes = "A2"
    wb.save(path)
    print(f"Wrote {path}", file=sys.stderr)


# ---------------------------------------------------------------------------
# 1. Current members of the chosen house
# ---------------------------------------------------------------------------
def fetch_current_members(house: str) -> list[dict]:
    """Return every current sitting member of the chosen house.

    `house` is "commons" or "lords".
    """
    house_code = {"commons": 1, "lords": 2}.get(house.lower())
    if house_code is None:
        raise ValueError(f"HOUSE must be 'commons' or 'lords', got {house!r}")

    members: list[dict] = []
    skip, take = 0, 20
    while True:
        data = api_get(
            f"{MEMBERS_API}/Members/Search",
            params={
                "House": house_code,
                "IsCurrentMember": "true",
                "skip": skip,
                "take": take,
            },
        )
        items = data.get("items") or []
        if not items:
            break
        for it in items:
            v = it.get("value") or it
            party = (v.get("latestParty") or {}).get("name")
            seat  = (v.get("latestHouseMembership") or {}).get("membershipFrom")
            members.append({
                "id":           v.get("id"),
                "name":         v.get("nameDisplayAs") or v.get("nameFullTitle"),
                "constituency": seat,
                "party":        party,
            })
        total = data.get("totalResults", 0)
        skip += take
        if skip >= total:
            break
        time.sleep(SLEEP)
    return members


# ---------------------------------------------------------------------------
# 2. EDM list (used both for full fetches and for incremental updates)
# ---------------------------------------------------------------------------
def fetch_edm_list(start_date: str, end_date: str | None) -> list[dict]:
    """Return summary records of EDMs tabled within [start_date, end_date]."""
    edms: list[dict] = []
    skip, take = 0, 100
    total: int | None = None
    while True:
        params = {
            "parameters.tabledStartDate": start_date,
            "parameters.skip": skip,
            "parameters.take": take,
        }
        if end_date:
            params["parameters.tabledEndDate"] = end_date
        data = api_get(f"{EDMS_API}/EarlyDayMotions/list", params=params)

        items = pick(data, "Response", "items") or []
        if not items:
            break
        for e in items:
            value = pick(e, "Value", default=None)
            edms.append(value if isinstance(value, dict) else e)

        paging = pick(data, "PagingInfo", default={}) or {}
        total  = pick(paging, "Total", "GlobalTotal", "TotalResults",
                      default=None) or total
        if total is None:
            total = skip + len(items) + take

        skip += take
        print(f"  fetched {len(edms)}/{total} EDM summaries", file=sys.stderr)
        if skip >= total:
            break
        time.sleep(SLEEP)
    return edms


# ---------------------------------------------------------------------------
# 3. Per-EDM detail fetch (with on-disk cache)
# ---------------------------------------------------------------------------
DETAIL_URL = "{base}/EarlyDayMotion/{eid}"


def _sponsor_name(s: dict) -> str | None:
    m = pick(s, "Member", default={}) or {}
    raw = pick(s, "Name", "ListAs") or pick(m, "Name", "ListAs")
    return strip_honorific(raw) if raw else raw


def _sponsor_id(s: dict) -> int | None:
    m = pick(s, "Member", default={}) or {}
    return pick(s, "MemberId") \
        or pick(m, "MnisId") \
        or pick(m, "Id", "id")


def fetch_edm_detail(edm_summary: dict) -> dict:
    """Fetch one EDM's full detail (with on-disk cache) and return a row dict
    matching SPONSORS_HEADER."""
    eid = pick(edm_summary, "Id", "EdmId", "id")
    cache_path = os.path.join(CACHE_DIR, f"{eid}.json")
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as cf:
            detail = json.load(cf)
    else:
        detail = api_get(DETAIL_URL.format(base=EDMS_API, eid=eid))
        with open(cache_path, "w", encoding="utf-8") as cf:
            json.dump(detail, cf)
        time.sleep(SLEEP)  # only delay on real network calls, not cache hits
    body = pick(detail, "Response", default=detail) or detail

    title = pick(body, "Title", default="") or ""
    date_tabled = pick(body, "DateTabled", default=None) \
               or pick(edm_summary, "DateTabled", default=None)
    if isinstance(date_tabled, str) and "T" in date_tabled:
        date_tabled = date_tabled.split("T", 1)[0]

    # A "prayer" EDM prays against a negative statutory instrument. The API
    # flags it via a non-null PrayingAgainstNegativeStatutoryInstrumentId.
    is_prayer = pick(body, "PrayingAgainstNegativeStatutoryInstrumentId",
                     default=None) is not None

    # The detail endpoint returns every signatory in `Sponsors` (ordered by
    # SponsoringOrder, with the primary sponsor at position 1). Withdrawn
    # signatures are flagged via IsWithdrawn and excluded here.
    sponsors_raw = pick(body, "Sponsors", default=[]) or []
    sponsors_raw = sorted(
        sponsors_raw,
        key=lambda s: pick(s, "SponsoringOrder", default=9999) or 9999,
    )

    # Tabler: prefer the PrimarySponsor object; fall back to SponsoringOrder=1.
    primary = pick(body, "PrimarySponsor", default={}) or {}
    tabler_id = _sponsor_id(primary) or pick(body, "MemberId")
    tabler_name = _sponsor_name(primary)
    if (tabler_id is None or not tabler_name) and sponsors_raw:
        first = sponsors_raw[0]
        tabler_id = tabler_id or _sponsor_id(first)
        tabler_name = tabler_name or _sponsor_name(first)

    sponsor_list: list[tuple[int, str]] = []
    seen: set[int] = set()
    for s in sponsors_raw:
        if pick(s, "IsWithdrawn", default=False):
            continue
        mid = _sponsor_id(s)
        if mid is None or mid in seen:
            continue
        seen.add(mid)
        sponsor_list.append((mid, _sponsor_name(s) or ""))

    ids_str   = ";".join(str(m) for m, _ in sponsor_list)
    names_str = ";".join(n for _, n in sponsor_list)

    print(f"  EDM {eid}: \"{title[:60]}\" "
          f"({len(sponsor_list)} signers, tabled {date_tabled})", file=sys.stderr)

    return {
        "edm_id":        eid,
        "date_tabled":   date_tabled,
        "is_prayer":     "yes" if is_prayer else "no",
        "title":         title,
        "tabler_id":     tabler_id,
        "tabler_name":   tabler_name,
        "sponsor_count": len(sponsor_list),
        "sponsor_ids":   ids_str,
        "sponsor_names": names_str,
    }


# ---------------------------------------------------------------------------
# 4. Incremental refresh of the sponsors master CSV
# ---------------------------------------------------------------------------
def load_existing_sponsors(path: str) -> tuple[list[dict], str | None]:
    """Load existing sponsors CSV as list of row-dicts, plus the latest
    date_tabled seen. Returns ([], None) if the file doesn't exist yet."""
    if not os.path.exists(path):
        return [], None
    rows: list[dict] = []
    max_date: str | None = None
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
            dt = r.get("date_tabled") or ""
            if dt and (max_date is None or dt > max_date):
                max_date = dt
    return rows, max_date


def refresh_sponsors_master(path: str,
                             start_date: str,
                             end_date: str | None) -> list[dict]:
    """Return a complete, up-to-date list of sponsor row-dicts.

    Reads the existing master CSV (if any), fetches only EDMs that are new
    since the last recorded date, writes the merged master CSV, and returns
    the merged rows in memory.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)

    existing_rows, existing_max_date = load_existing_sponsors(path)
    existing_ids = {r["edm_id"] for r in existing_rows}

    if existing_rows:
        # Re-fetch from the day of the latest existing EDM so we catch any
        # EDMs added to Parliament's system on that day after our last run.
        # Dedupe by edm_id afterwards.
        effective_start = max(start_date, existing_max_date or start_date)
        print(f"Master has {len(existing_rows)} EDMs (latest {existing_max_date}). "
              f"Fetching updates from {effective_start}...",
              file=sys.stderr)
    else:
        effective_start = start_date
        print(f"No existing master found. Fetching EDMs from {effective_start} "
              f"(this can take 30-60 minutes on first run).",
              file=sys.stderr)

    summary_list = fetch_edm_list(effective_start, end_date)
    new_summaries = [e for e in summary_list
                     if str(pick(e, "Id", "EdmId", "id")) not in existing_ids]

    if not new_summaries:
        print("  no new EDMs since last run.", file=sys.stderr)
    else:
        print(f"  {len(new_summaries)} new EDMs to fetch details for...",
              file=sys.stderr)

    new_rows: list[dict] = []
    for i, summary in enumerate(new_summaries, 1):
        row = fetch_edm_detail(summary)
        if i % 50 == 0:
            print(f"  ...processed {i}/{len(new_summaries)} new EDMs",
                  file=sys.stderr)
        new_rows.append(row)

    merged = existing_rows + new_rows
    # Sort by date_tabled descending (most recent first), then edm_id desc.
    merged.sort(key=lambda r: (r.get("date_tabled") or "",
                               str(r.get("edm_id") or "")),
                reverse=True)

    # Write merged master back out.
    merged_table = [[r.get(h, "") for h in SPONSORS_HEADER] for r in merged]
    write_csv(path, SPONSORS_HEADER, merged_table)

    return merged


# ---------------------------------------------------------------------------
# 5. Filtering + status CSV construction
# ---------------------------------------------------------------------------
def filters_are_active() -> bool:
    """True if any filter would narrow the output vs. the default run."""
    return (HOUSE.lower() != "commons"
            or PRAYER_MODE != "all"
            or PARTY_FILTER is not None
            or CONSTITUENCY_FILTER is not None
            or KEYWORD_FILTER is not None)


def derive_run_label() -> str | None:
    """Auto-derive a short filename-safe label from the active filters."""
    if RUN_LABEL:
        return re.sub(r"[^A-Za-z0-9_-]+", "_", RUN_LABEL).strip("_") or None
    parts: list[str] = []
    if HOUSE.lower() != "commons":
        parts.append(HOUSE.lower())
    if PRAYER_MODE != "all":
        parts.append(PRAYER_MODE)
    if PARTY_FILTER:
        parts.append("party-" + "-".join(PARTY_FILTER)[:30])
    if CONSTITUENCY_FILTER:
        parts.append("con-" + "-".join(CONSTITUENCY_FILTER)[:30])
    if KEYWORD_FILTER:
        parts.append("kw-" + "-".join(KEYWORD_FILTER)[:30])
    if not parts:
        return None
    label = "_".join(parts)
    return re.sub(r"[^A-Za-z0-9_-]+", "_", label).strip("_")


def status_output_path(extension: str = "csv") -> str:
    """Compute the output path for this run.

    * Default runs (no filters active) write to the stable
      edm_signing_status.csv at the top of databases/edm_data/, so
      downstream tools like update_mp_database.py can rely on that path
      without knowing the run's timestamp.
    * Filtered runs live inside their own subfolder named
      <label>_<YYYY-MM-DD_HH-MM-SS>/ beside the master CSV. The master
      edm_sponsors.csv is NOT duplicated into that subfolder -- it stays
      once at databases/edm_data/edm_sponsors.csv for every run.
    """
    label = derive_run_label()
    if not label:
        return f"{STATUS_BASENAME}.{extension}"
    parent_dir = os.path.dirname(STATUS_BASENAME)
    stem       = os.path.basename(STATUS_BASENAME)  # "edm_signing_status"
    run_folder = f"{label}_{_RUN_TIMESTAMP}"
    return os.path.join(parent_dir, run_folder, f"{stem}.{extension}")


def filter_members(members: list[dict]) -> list[dict]:
    """Apply PARTY_FILTER and CONSTITUENCY_FILTER to the members list.

    PARTY_FILTER matching is controlled by PARTY_FILTER_MODE:
      * "substring" (default): case-insensitive substring -- "Labour"
        matches both "Labour" and "Labour (Co-op)".
      * "exact": case-insensitive exact match -- "Labour" matches only
        "Labour", not "Labour (Co-op)".

    CONSTITUENCY_FILTER is a case-insensitive exact match -- constituency
    names are unambiguous and a substring match would over-include
    (e.g. "Cambridge" matching "Cambridgeshire South").
    """
    mode = (PARTY_FILTER_MODE or "substring").strip().lower()
    if mode not in ("substring", "exact"):
        print(f"  [warn] PARTY_FILTER_MODE={PARTY_FILTER_MODE!r} is not "
              f"'substring' or 'exact'; defaulting to 'substring'.",
              file=sys.stderr)
        mode = "substring"

    want_parties = [p.strip().lower() for p in PARTY_FILTER] if PARTY_FILTER else None
    want_cons    = {c.strip().lower() for c in CONSTITUENCY_FILTER} if CONSTITUENCY_FILTER else None

    out = []
    for m in members:
        if want_parties is not None:
            mp_party = (m.get("party") or "").strip().lower()
            if mode == "exact":
                if mp_party not in want_parties:
                    continue
            else:  # substring
                if not any(wp in mp_party for wp in want_parties):
                    continue
        if want_cons is not None:
            if (m.get("constituency") or "").strip().lower() not in want_cons:
                continue
        out.append(m)
    return out


def edm_matches_filters(row: dict) -> bool:
    """Return True if this EDM should count toward signing totals, given the
    active date window (START_DATE / END_DATE), PRAYER_MODE, and KEYWORD_FILTER.

    The master edm_sponsors.csv accumulates EVERY EDM we've ever fetched,
    so narrowing START_DATE / END_DATE at the CONFIG level has to be applied
    again here at count time -- otherwise counts would reflect the whole
    historical range, not the configured window.
    """
    # Date window.
    dt = (row.get("date_tabled") or "").strip()
    if dt:
        if START_DATE and dt < START_DATE:
            return False
        if END_DATE and dt > END_DATE:
            return False

    # Prayer filter.
    is_prayer = (row.get("is_prayer") or "").strip().lower() == "yes"
    if PRAYER_MODE == "exclude_prayers" and is_prayer:
        return False
    if PRAYER_MODE == "only_prayers" and not is_prayer:
        return False

    # Keyword / theme filter on title.
    if KEYWORD_FILTER:
        title = (row.get("title") or "").lower()
        if not any(k.lower() in title for k in KEYWORD_FILTER):
            return False

    return True


def build_counts_from_sponsors(sponsors_rows: list[dict]) -> tuple[Counter, Counter]:
    """Rebuild per-member signing counts from the master sponsors data,
    applying PRAYER_MODE and KEYWORD_FILTER. Also returns a parallel counter
    for non-prayer-only signatures (used only in the default 8-column schema)."""
    total: Counter = Counter()
    non_prayer: Counter = Counter()
    for r in sponsors_rows:
        is_prayer = (r.get("is_prayer") or "").strip().lower() == "yes"
        if not edm_matches_filters(r):
            continue
        ids_str = r.get("sponsor_ids") or ""
        for sid in ids_str.split(";"):
            sid = sid.strip()
            if not sid:
                continue
            try:
                mid = int(sid)
            except ValueError:
                continue
            total[mid] += 1
            if not is_prayer:
                non_prayer[mid] += 1
    return total, non_prayer


def build_status_table(members: list[dict],
                        sponsors_rows: list[dict]) -> tuple[list[str], list[list]]:
    """Construct the header + rows for the signing-status CSV.

    * When no filters are active, uses the 8-column original schema (total
      counts + non-prayer counts) for backward compatibility.
    * When any filter is active, uses a 6-column "matching EDMs" schema.
    """
    total_counts, non_prayer_counts = build_counts_from_sponsors(sponsors_rows)
    filtered_members = filter_members(members)

    # Safety-net diagnostic: if a party/constituency filter was set but the
    # result is empty, print the distinct values actually seen in the member
    # list so the user can spot a typo (e.g. "Green" vs "Green Party").
    if filters_are_active() and not filtered_members and members:
        if PARTY_FILTER:
            seen_parties = sorted({(m.get("party") or "").strip()
                                   for m in members if m.get("party")})
            print(f"  [warn] PARTY_FILTER {PARTY_FILTER} matched 0 MPs.",
                  file=sys.stderr)
            print(f"         Parties actually seen: {seen_parties}",
                  file=sys.stderr)
        if CONSTITUENCY_FILTER:
            seen_cons = sorted({(m.get("constituency") or "").strip()
                                for m in members if m.get("constituency")})
            print(f"  [warn] CONSTITUENCY_FILTER {CONSTITUENCY_FILTER} "
                  f"matched 0 MPs.", file=sys.stderr)
            print(f"         {len(seen_cons)} constituencies in data (first 20): "
                  f"{seen_cons[:20]}", file=sys.stderr)

    if not filters_are_active():
        header = ["member_id", "name", "constituency", "party",
                  "signed_edm_since_2024_07", "edms_signed_count",
                  "signed_edm_non_prayer_since_2024_07",
                  "edms_signed_non_prayer_count"]
        rows = []
        for mp in filtered_members:
            cleaned = strip_honorific(mp["name"])
            n_total     = total_counts.get(mp["id"], 0)
            n_nonprayer = non_prayer_counts.get(mp["id"], 0)
            rows.append([mp["id"], cleaned, mp["constituency"], mp["party"],
                         "yes" if n_total     > 0 else "no", n_total,
                         "yes" if n_nonprayer > 0 else "no", n_nonprayer])
    else:
        header = ["member_id", "name", "constituency", "party",
                  "signed_matching_edm", "matching_edms_signed_count"]
        rows = []
        for mp in filtered_members:
            cleaned = strip_honorific(mp["name"])
            n = total_counts.get(mp["id"], 0)
            rows.append([mp["id"], cleaned, mp["constituency"], mp["party"],
                         "yes" if n > 0 else "no", n])

    rows.sort(key=lambda r: (r[1] or "").lower())
    return header, rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def describe_config() -> None:
    """Print a short summary of the active CONFIG so the user can verify it."""
    print("--- Run configuration -------------------------------------------",
          file=sys.stderr)
    print(f"  Date window:     {START_DATE} -> "
          f"{END_DATE or 'today'}", file=sys.stderr)
    print(f"  House:           {HOUSE}", file=sys.stderr)
    print(f"  Prayer mode:     {PRAYER_MODE}", file=sys.stderr)
    party_mode_note = f" [{PARTY_FILTER_MODE}]" if PARTY_FILTER else ""
    print(f"  Party filter:    {PARTY_FILTER or '(all parties)'}{party_mode_note}",
          file=sys.stderr)
    print(f"  Constituency:    {CONSTITUENCY_FILTER or '(all constituencies)'}",
          file=sys.stderr)
    print(f"  Keyword filter:  {KEYWORD_FILTER or '(no keyword filter)'}",
          file=sys.stderr)
    print(f"  Status CSV:      {status_output_path('csv')}", file=sys.stderr)
    print("-----------------------------------------------------------------",
          file=sys.stderr)


def main() -> None:
    describe_config()

    # --- Fetch current members of the chosen house ------------------------
    print(f"Fetching current {HOUSE} members...", file=sys.stderr)
    members = fetch_current_members(HOUSE)
    print(f"  {len(members)} members", file=sys.stderr)

    # --- Refresh master EDM sponsors CSV (incremental) --------------------
    print("Refreshing master sponsors CSV...", file=sys.stderr)
    sponsors_rows = refresh_sponsors_master(SPONSORS_CSV, START_DATE, END_DATE)
    print(f"  master now contains {len(sponsors_rows)} EDMs", file=sys.stderr)

    # --- Apply filters and build this run's status CSV --------------------
    matching_count = sum(1 for r in sponsors_rows if edm_matches_filters(r))
    print(f"  {matching_count} EDMs match the active filters", file=sys.stderr)

    status_header, status_rows = build_status_table(members, sponsors_rows)
    status_csv = status_output_path("csv")
    write_csv(status_csv, status_header, status_rows)

    if WRITE_XLSX:
        # Pair each CSV with a matching .xlsx. The sponsors sheet always
        # reflects the master (unfiltered); the signing_status sheet
        # reflects the current filters.
        sponsors_table = [[r.get(h, "") for h in SPONSORS_HEADER]
                          for r in sponsors_rows]
        write_xlsx(
            status_output_path("xlsx"),
            {"sponsors": (SPONSORS_HEADER, sponsors_table),
             "signing_status": (status_header, status_rows)},
        )

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
