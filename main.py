"""
Watheeq Insurance Agent API - v1.0 (multi-tenant, self-owned Supabase)
Backs the Watheeq node of the T2 multi-agent WhatsApp workflow (motor, health,
travel, home). Pattern source: Al-Noor healthcare API v3.5 — same helpers,
same tenancy discipline, same auth token manager, same pruning. SPEC.md §1 + §3
is canonical for every table, column, enum, route, error code and formula.

DESIGN NOTES (v1.0):
  - ONE WORKHORSE. GET /customer returns everything the agent needs for a
    customer in one call: profile, rules, dependents, vehicles, policies
    (active/expired/cancelled), health members with remaining limits, claims
    (open/closed, with overdue computed server-side), pre-approvals, bookings,
    open quotes, payment requests (with pay_url), refunds, complaints and a
    summary. All related reads run in ONE asyncio.gather. The model never adds,
    subtracts or computes a weekday — every number and every date label it
    quotes is computed here (EN + AR).
  - FORMULAS ARE SERVER-SIDE. Every premium, pro-rata, refund, reimbursement
    and limit is computed by POST /quote or POST /claim/open with Decimal
    arithmetic rounded half-up to 2 dp, and returned with EN/AR breakdown lines
    the agent can quote verbatim. A quote is persisted (QTE-, 24 h validity) and
    POST /quote/apply executes exactly what was quoted — the price the customer
    agreed to is the price that is written.
  - STABLE ERROR CODES. 400 validation, 404 not found, 409 rule violation /
    duplicate. detail = {code, message, ...context}. The SI keys its failure
    templates on `code`, so codes never change between versions.
  - AVAILABILITY IS COMPUTED, NEVER STORED. Inspection / hospital / rental /
    home-inspection / dental slots are generated from today + rules minus
    existing bookings, so a cloned demo never goes stale. slot_ref is an opaque
    base64url token of `type|provider_or_centre|iso_start|doctor_id` that
    POST /booking/create decodes and re-validates.
  - WRITES ARE REAL. validate -> duplicate check -> entity exists -> write ->
    log_agent_action -> {ok: true, ...}. Nothing is faked; the portal mirrors
    every write through the agent_actions realtime feed.
  - DOCUMENTS are rendered on demand with reportlab (EN, or AR with the bundled
    Amiri font + arabic-reshaper + python-bidi), uploaded to Storage with
    x-upsert and returned as a 1 h signed URL ready for send_whatsapp_media.
  - CUSTOMER MEDIA (attachment_urls) is copied server-side into Storage. A
    failed download is recorded with stored=false — a media hiccup never fails
    the business write.

AUTHENTICATION (two modes, chosen automatically):
  Mode A (preferred — self-owned Supabase): SUPABASE_SERVICE_ROLE_KEY is set.
    Every PostgREST / Storage call carries the service-role key. No token
    lifecycle, RLS bypassed, tenancy enforced by this code (_scope_params).
  Mode B (fallback — Lovable Cloud style, copied from healthcare v3.x): no
    service key. Sign in as a dedicated service-account user with the password
    grant, cache the token, refresh 5 min before expiry, serialise refreshes
    behind an asyncio.Lock, and retry once on any 401/403. That account needs
    additive cross-tenant RLS policies on every per-tenant table.

TENANCY MODEL (playbook §3):
  Per-tenant tables (scoped by owner_id):
    customers, dependents, vehicles, policies, health_members, najm_reports,
    claims, valuation_disputes, preauths, bookings, quotes, payment_requests,
    refunds, complaints, agent_actions
  Shared tables (no owner_id, one copy for everyone, 60 s in-process cache):
    health_classes, providers, provider_doctors, service_centres, plans,
    addons, business_rules
  caller_phone -> owner_id via demo_users (5-min TTL cache, phone normalised),
  DEFAULT_OWNER_ID fallback. The public pay routes use the pay_token as scope.

ENV VARS:
  SUPABASE_URL               (required)  e.g. https://xxxx.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  (mode A)    service-role key — SECRET
  SUPABASE_ANON_KEY          (mode B)    publishable/anon key
  SUPABASE_SERVICE_EMAIL     (mode B)    service-account email
  SUPABASE_SERVICE_PASSWORD  (mode B)    service-account password — SECRET
  DEFAULT_OWNER_ID           (required)  UUID of the fallback demo tenant
  PORTAL_BASE_URL            (required)  e.g. https://watheeq-portal.lovable.app
  SUPABASE_REST_URL          (optional)  default {SUPABASE_URL}/rest/v1
  SUPABASE_STORAGE_URL       (optional)  default {SUPABASE_URL}/storage/v1
  DOCS_BUCKET                (optional)  default "documents"
  DEMO_LOCATION_SNAP         (optional)  default "true"
  TZ_NAME                    (optional)  default "Asia/Riyadh"
"""

import asyncio
import base64
import difflib
import io
import json
import math
import mimetypes
import os
import re
import secrets
import uuid
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from time import monotonic as _monotonic
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

API_VERSION = "1.0"
SERVICE_NAME = "Watheeq Insurance Agent API"


# ============================================================
# Config
# ============================================================

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
SUPABASE_SERVICE_EMAIL = os.environ.get("SUPABASE_SERVICE_EMAIL", "")
SUPABASE_SERVICE_PASSWORD = os.environ.get("SUPABASE_SERVICE_PASSWORD", "")
DEFAULT_OWNER_ID = os.environ.get("DEFAULT_OWNER_ID", "")
PORTAL_BASE_URL = os.environ.get("PORTAL_BASE_URL", "").rstrip("/")
SUPABASE_REST_URL = (os.environ.get("SUPABASE_REST_URL") or f"{SUPABASE_URL}/rest/v1").rstrip("/")
SUPABASE_STORAGE_URL = (os.environ.get("SUPABASE_STORAGE_URL") or f"{SUPABASE_URL}/storage/v1").rstrip("/")
DOCS_BUCKET = os.environ.get("DOCS_BUCKET", "documents")
DEMO_LOCATION_SNAP = os.environ.get("DEMO_LOCATION_SNAP", "true").lower() == "true"
TZ_NAME = os.environ.get("TZ_NAME", "Asia/Riyadh")
TZ = ZoneInfo(TZ_NAME)

HERE = os.path.dirname(os.path.abspath(__file__))
FONTS_DIR = os.path.join(HERE, "fonts")

# Mode A when the service-role key is present; otherwise Mode B.
AUTH_MODE = "service_role" if SUPABASE_SERVICE_ROLE_KEY else "service_account"

_required = [("SUPABASE_URL", SUPABASE_URL), ("DEFAULT_OWNER_ID", DEFAULT_OWNER_ID),
             ("PORTAL_BASE_URL", PORTAL_BASE_URL)]
if AUTH_MODE == "service_account":
    _required += [
        ("SUPABASE_ANON_KEY", SUPABASE_ANON_KEY),
        ("SUPABASE_SERVICE_EMAIL", SUPABASE_SERVICE_EMAIL),
        ("SUPABASE_SERVICE_PASSWORD", SUPABASE_SERVICE_PASSWORD),
    ]
_missing = [name for name, val in _required if not val]
if _missing:
    print(f"WARNING: missing required env vars: {', '.join(_missing)}. API will fail.")
print(f"[config] auth mode = {AUTH_MODE}; rest = {SUPABASE_REST_URL}; storage = {SUPABASE_STORAGE_URL}")


# ============================================================
# Supabase auth — Mode A (service role) / Mode B (service account)
# ============================================================
# Mode B token lifecycle (copied from healthcare v3.x):
#   startup            -> password grant, cache access + refresh token
#   < 5 min to expiry  -> refresh_token grant (cheap)
#   refresh rejected   -> fall back to a fresh password grant
#   PostgREST 401/403  -> force re-auth, retry the request once
# A single asyncio.Lock serialises all of the above so N concurrent requests
# trigger one token fetch rather than N. Mode A skips all of it: the service
# key IS the token and never expires.

_TOKEN_REFRESH_MARGIN = 300.0  # refresh when < 5 min of life remains

_auth_state: dict = {
    "access_token": None,
    "refresh_token": None,
    "expires_at": 0.0,       # monotonic deadline
    "last_error": None,
    "signed_in_at": None,    # wall-clock ISO, for diagnostics
}
_auth_lock: Optional[asyncio.Lock] = None  # created lazily (needs a loop)


def _apikey() -> str:
    return SUPABASE_SERVICE_ROLE_KEY if AUTH_MODE == "service_role" else SUPABASE_ANON_KEY


async def _auth_request(payload: dict, grant_type: str) -> dict:
    """POST to Supabase's token endpoint. Raises on failure."""
    url = f"{SUPABASE_URL}/auth/v1/token?grant_type={grant_type}"
    r = await http_client.post(
        url,
        headers={"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"},
        json=payload,
    )
    r.raise_for_status()
    return r.json()


async def _sign_in_password() -> None:
    """Full sign-in with email + password. Replaces any cached token."""
    data = await _auth_request(
        {"email": SUPABASE_SERVICE_EMAIL, "password": SUPABASE_SERVICE_PASSWORD},
        "password",
    )
    _store_token(data)
    print(f"[auth] signed in as {SUPABASE_SERVICE_EMAIL}")


async def _sign_in_refresh() -> None:
    """Renew using the refresh token. Cheaper than a password grant."""
    rt = _auth_state.get("refresh_token")
    if not rt:
        raise RuntimeError("no refresh token cached")
    data = await _auth_request({"refresh_token": rt}, "refresh_token")
    _store_token(data)
    print("[auth] token refreshed")


def _store_token(data: dict) -> None:
    expires_in = float(data.get("expires_in") or 3600)
    _auth_state["access_token"] = data.get("access_token")
    _auth_state["refresh_token"] = data.get("refresh_token") or _auth_state.get("refresh_token")
    _auth_state["expires_at"] = _monotonic() + expires_in
    _auth_state["last_error"] = None
    _auth_state["signed_in_at"] = datetime.now().astimezone().isoformat()


async def ensure_token(force: bool = False) -> str:
    """Return a valid bearer token.

    Mode A: the service-role key, always. Mode B: cached user token, refreshed
    or re-signed-in as needed; `force=True` discards the cache (after a 401).
    Serialised by _auth_lock so concurrent callers share one fetch.
    """
    if AUTH_MODE == "service_role":
        return SUPABASE_SERVICE_ROLE_KEY

    global _auth_lock
    if _auth_lock is None:
        _auth_lock = asyncio.Lock()

    tok = _auth_state.get("access_token")
    fresh_enough = tok and (_auth_state["expires_at"] - _monotonic()) > _TOKEN_REFRESH_MARGIN
    if fresh_enough and not force:
        return tok

    async with _auth_lock:
        # Re-check inside the lock: another coroutine may have just refreshed.
        tok = _auth_state.get("access_token")
        fresh_enough = tok and (_auth_state["expires_at"] - _monotonic()) > _TOKEN_REFRESH_MARGIN
        if fresh_enough and not force:
            return tok
        try:
            if force:
                await _sign_in_password()
            else:
                try:
                    await _sign_in_refresh()
                except Exception:
                    await _sign_in_password()
        except Exception as e:
            _auth_state["last_error"] = str(e)[:300]
            print(f"[auth] sign-in FAILED: {e}")
            raise HTTPException(
                status_code=502,
                detail={"code": "db_auth_failed",
                        "message": "Database authentication failed — check service account credentials"},
            )
        return _auth_state["access_token"]


async def sb_headers(extra: Optional[dict] = None) -> dict:
    """Build request headers with a currently-valid bearer token."""
    token = await ensure_token()
    h = {
        "apikey": _apikey(),
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    if extra:
        h.update(extra)
    return h


def _auth_stats() -> dict:
    """Diagnostic: token state, exposed via /health. Never leaks the token."""
    if AUTH_MODE == "service_role":
        return {"mode": AUTH_MODE, "authenticated": bool(SUPABASE_SERVICE_ROLE_KEY),
                "expires_in_seconds": None, "signed_in_at": None, "last_error": None}
    exp = _auth_state.get("expires_at") or 0
    return {
        "mode": AUTH_MODE,
        "authenticated": bool(_auth_state.get("access_token")),
        "expires_in_seconds": round(exp - _monotonic(), 1) if exp else None,
        "signed_in_at": _auth_state.get("signed_in_at"),
        "last_error": _auth_state.get("last_error"),
    }


# ============================================================
# Tenancy: which tables carry owner_id
# ============================================================
# Per-tenant tables get `owner_id=eq.<uuid>` injected into every query and
# `owner_id` injected into every inserted row. Shared tables have no owner_id.

TENANT_TABLES = {
    "customers",
    "dependents",
    "vehicles",
    "policies",
    "health_members",
    "najm_reports",
    "claims",
    "valuation_disputes",
    "preauths",
    "bookings",
    "quotes",
    "payment_requests",
    "refunds",
    "complaints",
    "agent_actions",
}


# ============================================================
# App
# ============================================================

app = FastAPI(title=SERVICE_NAME, version=API_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared httpx client for connection pooling
http_client: Optional[httpx.AsyncClient] = None


@app.on_event("startup")
async def startup():
    global http_client
    # HTTP/2 multiplexes the workhorse's ~15 parallel queries over ONE TCP
    # connection. On HTTP/1.1 they would queue behind the ~6-connection limit.
    # Requires the h2 package (httpx[http2] in requirements.txt).
    http_client = httpx.AsyncClient(
        http2=True,
        timeout=30.0,
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
    )
    _register_fonts()
    # Mode B: sign in up front so the first real request doesn't pay for it.
    # Don't crash on failure — /health reports it and every request retries.
    if AUTH_MODE == "service_account":
        try:
            await ensure_token(force=True)
        except Exception as e:
            print(f"[auth] startup sign-in failed (will retry on first request): {e}")


@app.on_event("shutdown")
async def shutdown():
    global http_client
    if http_client:
        await http_client.aclose()


# ============================================================
# Errors — stable {code, message, ...context} details
# ============================================================

def err(http_status: int, code: str, message: str, /, **context) -> HTTPException:
    """Build an HTTPException whose detail the SI can key on (`code`).
    Positional-only so context may itself carry a `status` key."""
    return HTTPException(status_code=http_status, detail={"code": code, "message": message, **context})


@app.exception_handler(HTTPException)
async def _http_exc_handler(request, exc: HTTPException):
    # FastAPI's default handler already returns {"detail": ...}; normalise plain
    # string details into the {code, message} shape so every error is keyable.
    detail = exc.detail
    if isinstance(detail, str):
        detail = {"code": {400: "bad_request", 404: "not_found", 409: "conflict"}.get(exc.status_code, "error"),
                  "message": detail}
    return JSONResponse(status_code=exc.status_code, content={"detail": detail})


@app.exception_handler(RequestValidationError)
async def _validation_handler(request: Request, exc: RequestValidationError):
    """FastAPI's default 422 has no `code`. The SI keys on codes, so body/query
    validation failures come back in the same {code, message} shape as every
    other 400 (mirrors Barq)."""
    errors = []
    for e in exc.errors():
        field = ".".join(str(p) for p in e.get("loc", []) if p not in ("body", "query"))
        errors.append({"field": field, "message": str(e.get("msg", "")), "type": str(e.get("type", ""))})
    fields = [e["field"] for e in errors if e["field"]]
    return JSONResponse(status_code=400, content={"detail": {
        "code": "invalid_request",
        "message": ("Invalid or missing fields: " + ", ".join(fields)) if fields else "Invalid request body",
        "errors": errors,
    }})


# ============================================================
# Phone normalization + tenant resolution
# ============================================================

def normalize_phone(raw: Optional[str]) -> Optional[str]:
    """Canonicalize a phone number to E.164 with a leading '+'.

    Tolerates: missing '+', spaces, dashes, parentheses, leading '00'.
    URL query strings turn '+' into a space and agents sometimes strip it;
    normalizing on both sides means the lookup matches regardless.
    """
    if not raw:
        return None
    s = re.sub(r"[\s\-()]", "", str(raw).strip())
    if not s:
        return None
    if s.startswith("00"):
        s = "+" + s[2:]
    elif not s.startswith("+"):
        s = "+" + s
    if not re.fullmatch(r"\+\d{6,20}", s):
        return None
    return s


_TENANT_CACHE_TTL = 300.0  # 5 minutes
_tenant_cache: dict = {}  # normalized_phone -> {"owner_id": str, "ts": float}


async def resolve_owner(caller_phone: Optional[str]) -> str:
    """Resolve a WhatsApp phone number to the owning demo tenant's owner_id.

    Falls back to DEFAULT_OWNER_ID when caller_phone is missing or not
    registered in demo_users. A demo that lands in the shared default tenant is
    recoverable; a hard 400 mid-demo in front of a prospect is not.
    """
    normalized = normalize_phone(caller_phone)
    if not normalized:
        return DEFAULT_OWNER_ID

    cached = _tenant_cache.get(normalized)
    if cached and (_monotonic() - cached["ts"]) <= _TENANT_CACHE_TTL:
        return cached["owner_id"]

    # demo_users is NOT a per-tenant table — it's the tenant registry itself.
    rows = await _sb_raw_get("demo_users", {
        "whatsapp_number": f"eq.{normalized}",
        "select": "owner_id",
        "limit": "1",
    })
    owner = rows[0]["owner_id"] if rows else DEFAULT_OWNER_ID
    _tenant_cache[normalized] = {"owner_id": owner, "ts": _monotonic()}
    return owner


def _tenant_cache_stats() -> dict:
    now = _monotonic()
    return {
        "entries": len(_tenant_cache),
        "oldest_age_seconds": (
            round(now - min(v["ts"] for v in _tenant_cache.values()), 1) if _tenant_cache else None
        ),
    }


# ============================================================
# Supabase REST helpers (tenant-aware)
# ============================================================

AUTH_FAIL_CODES = (401, 403)


async def _sb_request(method: str, table: str, *, params=None, json_body=None, extra_headers=None):
    """Execute one PostgREST call with a valid token, retrying once on 401/403.

    In Mode B a 401 means the token expired or was evicted — re-auth and retry.
    If it fails a second time the credentials or RLS are genuinely wrong and we
    surface that rather than hiding it. (Mode A: the retry is harmless.)
    """
    url = f"{SUPABASE_REST_URL}/{table}"
    for attempt in (1, 2):
        headers = await sb_headers(extra_headers)
        r = await http_client.request(method, url, headers=headers, params=params or {}, json=json_body)
        if r.status_code in AUTH_FAIL_CODES and attempt == 1 and AUTH_MODE == "service_account":
            print(f"[auth] {r.status_code} on {method} {table} — re-authenticating and retrying")
            await ensure_token(force=True)
            continue
        r.raise_for_status()
        if not r.content:
            return []
        return r.json()


async def _sb_raw_get(table: str, params: Optional[dict] = None) -> list:
    """Unscoped GET. ONLY for non-tenant tables (demo_users, shared catalogs)
    and the token-scoped public pay page.

    Auth failures are NOT swallowed: an empty list is a legitimate answer, a
    401 never is — returning [] on a 401 is what once made a misconfigured key
    look like "not found" for hours.
    """
    try:
        return await _sb_request("GET", table, params=params or {})
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        print(f"sb_get error {table}: {code} {e.response.text[:200]}")
        if code in AUTH_FAIL_CODES:
            raise HTTPException(status_code=502, detail={
                "code": "db_auth_failed",
                "message": f"Database authentication failed — API cannot read '{table}'"})
        raise HTTPException(status_code=502, detail={
            "code": "upstream_error", "message": f"Read of {table} failed: {e.response.text[:200]}"})
    except HTTPException:
        raise
    except Exception as e:
        print(f"sb_get exception {table}: {e}")
        raise HTTPException(status_code=502, detail={"code": "upstream_error",
                                                     "message": f"Read of {table} failed: {str(e)[:200]}"})


def _scope_params(table: str, params: Optional[dict], owner: Optional[str]) -> dict:
    """Inject owner_id filter for per-tenant tables.

    Raises loudly if a per-tenant table is queried without an owner. Silent
    cross-tenant reads are the worst possible failure mode here — better to
    500 and see it in the logs than to serve another sales person's demo data.
    """
    p = dict(params or {})
    if table in TENANT_TABLES:
        if not owner:
            raise HTTPException(status_code=500, detail={
                "code": "missing_owner_scope",
                "message": f"Internal error: query on per-tenant table '{table}' missing owner scope"})
        p["owner_id"] = f"eq.{owner}"
    return p


async def sb_get(table: str, params: Optional[dict] = None, owner: Optional[str] = None) -> list:
    """GET from Supabase REST, scoped to the tenant for per-tenant tables."""
    return await _sb_raw_get(table, _scope_params(table, params, owner))


async def sb_get_one(table: str, params: Optional[dict] = None, owner: Optional[str] = None) -> Optional[dict]:
    """GET single row. Returns dict or None."""
    p = dict(params or {})
    p.setdefault("limit", "1")
    rows = await sb_get(table, p, owner=owner)
    return rows[0] if rows else None


async def sb_insert(table: str, payload, owner: Optional[str] = None) -> Any:
    """INSERT, injecting owner_id for per-tenant tables. Dict or list of dicts."""
    if table in TENANT_TABLES:
        if not owner:
            raise HTTPException(status_code=500, detail={
                "code": "missing_owner_scope",
                "message": f"Internal error: insert into per-tenant table '{table}' missing owner scope"})
        if isinstance(payload, list):
            payload = [{**row, "owner_id": owner} for row in payload]
        else:
            payload = {**payload, "owner_id": owner}
    try:
        return await _sb_request("POST", table, json_body=payload)
    except httpx.HTTPStatusError as e:
        print(f"sb_insert error {table}: {e.response.status_code} {e.response.text[:300]}")
        raise HTTPException(status_code=502, detail={
            "code": "upstream_error", "message": f"Insert to {table} failed: {e.response.text[:200]}"})


async def sb_update(table: str, params: dict, payload: dict, owner: Optional[str] = None) -> Any:
    """UPDATE rows matching params, scoped to the tenant for per-tenant tables."""
    scoped = _scope_params(table, params, owner)
    try:
        return await _sb_request("PATCH", table, params=scoped, json_body=payload)
    except httpx.HTTPStatusError as e:
        print(f"sb_update error {table}: {e.response.status_code} {e.response.text[:300]}")
        raise HTTPException(status_code=502, detail={
            "code": "upstream_error", "message": f"Update {table} failed: {e.response.text[:200]}"})


async def sb_delete(table: str, params: dict, owner: Optional[str] = None) -> Any:
    """DELETE rows matching params, scoped to the tenant for per-tenant tables."""
    scoped = _scope_params(table, params, owner)
    try:
        return await _sb_request("DELETE", table, params=scoped)
    except httpx.HTTPStatusError as e:
        print(f"sb_delete error {table}: {e.response.status_code} {e.response.text[:300]}")
        raise HTTPException(status_code=502, detail={
            "code": "upstream_error", "message": f"Delete from {table} failed: {e.response.text[:200]}"})


async def log_agent_action(
    customer_id: Optional[str],
    action_type: str,
    description: str,
    metadata: Optional[dict] = None,
    owner: Optional[str] = None,
    reference_id: Optional[str] = None,
    status: str = "Success",
    source: str = "Agent",
):
    """Insert into agent_actions for the portal's Live Activity Drawer.
    `description` is the human line staff read. Tenant-scoped. Never raises —
    an audit failure must not break the write that already happened."""
    try:
        await sb_insert("agent_actions", {
            "customer_id": customer_id,
            "reference_id": reference_id,
            "action_type": action_type,
            "description": description,
            "metadata": metadata or {},
            "status": status,
            "source": source,
        }, owner=owner)
    except Exception as e:
        print(f"agent_actions log failed: {e}")


# ============================================================
# In-process cache for SHARED reference tables (60 s)
# ============================================================
# Shared catalogs are one copy for every tenant, read-only, and tiny. Serving
# them from process memory keeps the workhorse's gather at the per-tenant
# queries only. Global, not per-tenant; nothing to invalidate on reset.

_REF_CACHE_TTL = 60.0  # seconds
_REF_TABLES = {
    "health_classes": "class_code",
    "providers": "provider_id",
    "provider_doctors": "doctor_id",
    "service_centres": "centre_id",
    "plans": "plan_code",
    "addons": "addon_code",
    "business_rules": "rule_key",
}
_reference_cache: dict = {t: {"data": None, "ts": 0.0} for t in _REF_TABLES}


async def get_ref(table: str) -> list:
    """Return a shared catalog from cache, or fetch + cache it."""
    c = _reference_cache[table]
    if c["data"] is None or (_monotonic() - c["ts"]) > _REF_CACHE_TTL:
        c["data"] = await sb_get(table, {"select": "*", "order": f"{_REF_TABLES[table]}.asc"})
        c["ts"] = _monotonic()
    return c["data"]


async def get_ref_map(table: str) -> dict:
    """Shared catalog keyed by its primary key."""
    key = _REF_TABLES[table]
    return {r[key]: r for r in await get_ref(table)}


async def get_rules() -> dict:
    """business_rules as a flat {rule_key: value} dict (numbers as int/float,
    text rules as strings). The SI quotes rules ONLY from this dict."""
    out = {}
    for r in await get_ref("business_rules"):
        v = r.get("value_num")
        if v is None:
            v = r.get("value_text")
        else:
            v = _num(v)
        out[r["rule_key"]] = v
    return out


def _cache_stats() -> dict:
    now = _monotonic()
    return {
        table: {
            "warm": entry["data"] is not None,
            "row_count": len(entry["data"]) if entry["data"] is not None else 0,
            "age_seconds": round(now - entry["ts"], 1) if entry["ts"] else None,
        }
        for table, entry in _reference_cache.items()
    }


# ============================================================
# Response pruning
# ============================================================
# The workhorse response is re-sent to the model on every tool-loop iteration.
# owner_id/created_at/updated_at are tenancy plumbing the agent must never use.

_NOISE_KEYS = ("owner_id", "created_at", "updated_at")


def _strip(rows, *extra_keys):
    drop = set(_NOISE_KEYS) | set(extra_keys)
    return [{k: v for k, v in r.items() if k not in drop} for r in (rows or [])]


def _strip_one(row, *extra_keys):
    if not row:
        return row
    drop = set(_NOISE_KEYS) | set(extra_keys)
    return {k: v for k, v in row.items() if k not in drop}


# ============================================================
# Numbers, time and bilingual labels
# ============================================================

def _num(v) -> Any:
    """PostgREST numeric -> int when integral, else float. None stays None."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return v
    return int(f) if f.is_integer() else f


def D(v) -> Decimal:
    return Decimal(str(v if v is not None else 0))


def money(v) -> float:
    """Round half-up to 2 dp and return a float (JSON-friendly)."""
    return float(D(v).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def prorata(annual, days: int) -> float:
    """annual × days / 365, rounded half-up to 2 dp (one rounding, at the end)."""
    return money(D(annual) * Decimal(int(days)) / Decimal(365))


def fmt_sar(v) -> str:
    """'1,285.64' / '480' — for breakdown text lines."""
    m = D(money(v))
    return f"{m:,.2f}" if m != m.to_integral() else f"{int(m):,}"


def now_local() -> datetime:
    return datetime.now(TZ)


def today_local() -> date:
    return now_local().date()


def parse_date(v) -> Optional[date]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v.astimezone(TZ).date()
    if isinstance(v, date):
        return v
    s = str(v)
    try:
        if len(s) > 10:
            return parse_ts(s).date()
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def parse_ts(v) -> Optional[datetime]:
    """Parse a PostgREST timestamptz (or naive ISO) into an aware Riyadh datetime."""
    if not v:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        s = str(v).strip().replace(" ", "T", 1)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # Python 3.11 handles most shapes; normalise short "+03" offsets.
        s = re.sub(r"([+-]\d{2})$", r"\1:00", s)
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            try:
                dt = datetime.combine(date.fromisoformat(s[:10]), dtime(0, 0))
            except ValueError:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)


def iso(dt: datetime) -> str:
    return dt.astimezone(TZ).isoformat(timespec="seconds")


EN_WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
AR_WEEKDAYS = ["الاثنين", "الثلاثاء", "الأربعاء", "الخميس", "الجمعة", "السبت", "الأحد"]
EN_MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August",
             "September", "October", "November", "December"]
AR_MONTHS = ["يناير", "فبراير", "مارس", "أبريل", "مايو", "يونيو", "يوليو", "أغسطس",
             "سبتمبر", "أكتوبر", "نوفمبر", "ديسمبر"]


def date_labels(d: Optional[date], with_year: Optional[bool] = None) -> tuple:
    """('Sunday, 4 October', 'الأحد 4 أكتوبر'). Year appended when the date is
    far from today (> 300 days either way) or explicitly requested."""
    if not d:
        return (None, None)
    if with_year is None:
        with_year = abs((d - today_local()).days) > 300
    en = f"{EN_WEEKDAYS[d.weekday()]}, {d.day} {EN_MONTHS[d.month - 1]}"
    ar = f"{AR_WEEKDAYS[d.weekday()]} {d.day} {AR_MONTHS[d.month - 1]}"
    if with_year:
        en += f" {d.year}"
        ar += f" {d.year}"
    return (en, ar)


def short_date_labels(d: Optional[date], with_year: Optional[bool] = None) -> tuple:
    """('2 March', '2 مارس') — for 'ends 2 March' style quotes."""
    if not d:
        return (None, None)
    if with_year is None:
        with_year = abs((d - today_local()).days) > 300
    en = f"{d.day} {EN_MONTHS[d.month - 1]}"
    ar = f"{d.day} {AR_MONTHS[d.month - 1]}"
    if with_year:
        en += f" {d.year}"
        ar += f" {d.year}"
    return (en, ar)


def time_labels(dt: datetime) -> tuple:
    """('11:00 AM', '11:00 ص')."""
    dt = dt.astimezone(TZ)
    h12 = dt.hour % 12 or 12
    ampm_en = "AM" if dt.hour < 12 else "PM"
    ampm_ar = "ص" if dt.hour < 12 else "م"
    return (f"{h12}:{dt.minute:02d} {ampm_en}", f"{h12}:{dt.minute:02d} {ampm_ar}")


def relative_day_labels(d: date) -> tuple:
    delta = (d - today_local()).days
    if delta == 0:
        return ("today", "اليوم")
    if delta == 1:
        return ("tomorrow", "غداً")
    if delta == -1:
        return ("yesterday", "أمس")
    return (None, None)


def dt_labels(dt: Optional[datetime]) -> tuple:
    """('Sunday, 4 October, 11:00 AM', 'الأحد 4 أكتوبر، 11:00 ص'), prefixed
    with Today/Tomorrow/Yesterday when applicable."""
    if not dt:
        return (None, None)
    dt = dt.astimezone(TZ)
    den, dar = date_labels(dt.date())
    ten, tar = time_labels(dt)
    ren, rar = relative_day_labels(dt.date())
    en = f"{den}, {ten}"
    ar = f"{dar}، {tar}"
    if ren:
        en = f"{ren.capitalize()} ({den}), {ten}"
        ar = f"{rar} ({dar})، {tar}"
    return (en, ar)


def put_labels(obj: dict, key: str, value, kind: str = "date") -> dict:
    """Add `<key>_label_en/_ar` for a date (or timestamp) field in-place."""
    if kind == "datetime":
        en, ar = dt_labels(parse_ts(value))
    else:
        en, ar = date_labels(parse_date(value))
    obj[f"{key}_label_en"] = en
    obj[f"{key}_label_ar"] = ar
    return obj


def is_working_day(d: date) -> bool:
    """Saudi working week: Sunday–Thursday (Friday=4, Saturday=5 off)."""
    return d.weekday() not in (4, 5)


def add_working_days(start: date, n: int) -> date:
    """The n-th working day after `start` (start itself not counted)."""
    d = start
    added = 0
    while added < n:
        d += timedelta(days=1)
        if is_working_day(d):
            added += 1
    return d


def next_working_days(start: date, n: int, include_start: bool = False) -> list:
    out = []
    d = start if include_start else start + timedelta(days=1)
    while len(out) < n:
        if is_working_day(d):
            out.append(d)
        d += timedelta(days=1)
    return out


# ============================================================
# Geo: distances, labels, demo location snapping (§1.5)
# ============================================================

def haversine_m(lat1, lng1, lat2, lng2) -> float:
    r = 6371000.0
    p1, p2 = math.radians(float(lat1)), math.radians(float(lat2))
    dp = p2 - p1
    dl = math.radians(float(lng2) - float(lng1))
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def distance_labels(m: float) -> tuple:
    if m < 1000:
        v = int(round(m / 10.0) * 10)
        return (f"{v} m", f"{v} م")
    km = round(m / 1000.0, 1)
    km_s = f"{km:.1f}".rstrip("0").rstrip(".")
    return (f"{km_s} km", f"{km_s} كم")


# City centroids for the snapping test. Reference-data coordinates (providers,
# service centres) are added at runtime, so any real Watheeq city counts.
_CITY_CENTROIDS = [
    (24.7136, 46.6753),  # Riyadh
    (21.5433, 39.1728),  # Jeddah
    (26.4207, 50.0888),  # Dammam
]
SNAP_RADIUS_M = 60000.0


async def snap_location(lat, lng, anchor: tuple) -> tuple:
    """Return (lat, lng, snapped). A pin > 60 km from every city centroid in the
    reference data is replaced by `anchor` (the customer's home) when
    DEMO_LOCATION_SNAP=true, so a presenter outside KSA still gets Riyadh
    results. Missing lat/lng -> anchor, not flagged as snapped."""
    if lat is None or lng is None:
        return (anchor[0], anchor[1], False)
    try:
        lat, lng = float(lat), float(lng)
    except (TypeError, ValueError):
        return (anchor[0], anchor[1], True)
    if not DEMO_LOCATION_SNAP:
        return (lat, lng, False)
    points = list(_CITY_CENTROIDS)
    for t in ("providers", "service_centres"):
        for r in await get_ref(t):
            if r.get("lat") is not None and r.get("lng") is not None:
                points.append((float(r["lat"]), float(r["lng"])))
    if any(haversine_m(lat, lng, p[0], p[1]) <= SNAP_RADIUS_M for p in points):
        return (lat, lng, False)
    return (anchor[0], anchor[1], True)


RIYADH = (24.7136, 46.6753)


def home_of(customer: Optional[dict]) -> tuple:
    if customer and customer.get("home_lat") is not None and customer.get("home_lng") is not None:
        return (float(customer["home_lat"]), float(customer["home_lng"]))
    return RIYADH


# ============================================================
# IDs (tenant-scoped max+1 per prefix — playbook §4)
# ============================================================
# Each tenant counts independently, so CLM-33891 in Alice's demo and in Bob's
# demo are different claims — intentional, they are separate demos. First IDs
# in an empty prefix start at the numbers used in the T2 use-case script so a
# fresh demo reads the same as the storyboard (TPC-7740, RMB-4432, …).

ID_DEFAULTS = {
    # prefix: (width, first number)
    "CLM-": (5, 33900), "TPC-": (4, 7740), "RMB-": (4, 4432), "TRV-": (4, 2208),
    "PRP-": (4, 1094), "DSP-": (4, 1187), "CMP-": (4, 5521), "OBJ-": (4, 3310),
    "BKG-": (5, 50001), "QTE-": (5, 70001), "PAY-": (5, 80001), "RFD-": (5, 90001),
    "DEP-": (5, 50001), "MBR-": (5, 50001), "APR-": (5, 66200),
    "POL-MTR-": (5, 50001), "POL-HLT-": (5, 50001), "POL-TRV-": (5, 50001), "POL-HOM-": (5, 50001),
    "WTQ-HC-": (8, 10000001),
}


async def next_id(table: str, id_col: str, prefix: str, owner: str, count: int = 1):
    """Next business id(s) for `prefix` within this tenant: max(existing)+1."""
    width, first = ID_DEFAULTS[prefix]
    rows = await sb_get(table, {"select": id_col, id_col: f"like.{prefix}*"}, owner=owner)
    pat = re.compile(re.escape(prefix) + r"(\d+)$")
    nums = [int(m.group(1)) for r in rows if (m := pat.match(str(r.get(id_col) or "")))]
    n = (max(nums) + 1) if nums else first
    ids = [f"{prefix}{n + i:0{width}d}" for i in range(count)]
    return ids if count > 1 else ids[0]


def claim_prefix(claim_type: str) -> str:
    return {"Motor": "CLM-", "Third Party": "TPC-", "Health Reimbursement": "RMB-",
            "Travel": "TRV-", "Home": "PRP-"}[claim_type]


def policy_prefix(product: str) -> str:
    return {"Motor": "POL-MTR-", "Health": "POL-HLT-", "Travel": "POL-TRV-", "Home": "POL-HOM-"}[product]


# ============================================================
# Validation helpers: national ID, IBAN, names
# ============================================================

def valid_national_id(v: Optional[str]) -> bool:
    """Saudi national ID / iqama: 10 digits starting 1 (citizen) or 2 (resident)."""
    return bool(v) and bool(re.fullmatch(r"[12]\d{9}", str(v).strip()))


def mask_national_id(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = str(v)
    return "*" * max(0, len(s) - 4) + s[-4:]


def normalize_iban(v: Optional[str]) -> str:
    return re.sub(r"[\s\-]", "", str(v or "")).upper()


def iban_mod97_ok(iban: str) -> bool:
    """ISO 13616: move the first 4 chars to the end, letters -> 10..35, mod 97 == 1."""
    s = iban[4:] + iban[:4]
    digits = "".join(str(int(ch, 36)) for ch in s)
    return int(digits) % 97 == 1


def valid_saudi_iban(v: Optional[str]) -> bool:
    """SA + 22 digits (24 chars) with a valid ISO 13616 mod-97 checksum."""
    s = normalize_iban(v)
    return bool(re.fullmatch(r"SA\d{22}", s)) and iban_mod97_ok(s)


def mask_iban(v: Optional[str]) -> Optional[str]:
    s = normalize_iban(v)
    if not s:
        return None
    return f"{s[:4]} **** **** **** **** {s[-4:]}"


_NAME_NOISE = re.compile(r"[^a-z؀-ۿ ]+")


def _name_tokens(name: str) -> list:
    s = (name or "").lower().replace("-", " ").replace("’", "").replace("'", "")
    s = _NAME_NOISE.sub(" ", s)
    toks = [t for t in s.split() if t]
    # Drop the "al" / "ال" particle so "Al-Dosari" == "Aldosari" == "Dosari".
    out = []
    for t in toks:
        if t in ("al", "el", "bin", "ibn", "bint", "ال", "بن", "بنت"):
            continue
        if t.startswith("al") and len(t) > 4 and t not in ("ali", "alia", "alya"):
            t = t[2:]
        if t.startswith("ال") and len(t) > 3:
            t = t[2:]
        out.append(t)
    return out


def names_match(a: Optional[str], b_candidates: list) -> bool:
    """Fuzzy holder-name match: every token of the holder name must match a
    token of one candidate (difflib ratio ≥ 0.8), with at least two tokens, or
    the whole normalised strings are ≥ 0.85 similar."""
    ta = _name_tokens(a or "")
    if not ta:
        return False
    for b in b_candidates:
        tb = _name_tokens(b or "")
        if not tb:
            continue
        whole = difflib.SequenceMatcher(None, " ".join(ta), " ".join(tb)).ratio()
        if whole >= 0.85:
            return True
        hits = sum(1 for x in ta if any(difflib.SequenceMatcher(None, x, y).ratio() >= 0.8 for y in tb))
        if len(ta) >= 2 and hits == len(ta):
            return True
    return False


def mask_person_name(name: Optional[str]) -> Optional[str]:
    """'Mansour Al-Rashid' -> 'M*** Al-R*****'. Keeps the 'Al-' particle."""
    if not name:
        return None
    out = []
    for tok in name.split():
        m = re.match(r"^(Al-|al-|El-|el-)(.+)$", tok)
        if m:
            rest = m.group(2)
            out.append(m.group(1) + rest[0] + "*" * (len(rest) - 1))
        elif tok:
            out.append(tok[0] + "***")
    return " ".join(out)


# ============================================================
# Storage: uploads, signed URLs, customer media
# ============================================================

async def _storage_headers(extra: Optional[dict] = None) -> dict:
    token = await ensure_token()
    h = {"apikey": _apikey(), "Authorization": f"Bearer {token}"}
    if extra:
        h.update(extra)
    return h


async def storage_upload(path: str, data: bytes, content_type: str, bucket: str = None) -> str:
    """Upload bytes to Storage (x-upsert). Returns the object path."""
    bucket = bucket or DOCS_BUCKET
    url = f"{SUPABASE_STORAGE_URL}/object/{bucket}/{path}"
    for attempt in (1, 2):
        headers = await _storage_headers({"Content-Type": content_type, "x-upsert": "true",
                                          "cache-control": "max-age=60"})
        r = await http_client.post(url, headers=headers, content=data)
        if r.status_code in AUTH_FAIL_CODES and attempt == 1 and AUTH_MODE == "service_account":
            await ensure_token(force=True)
            continue
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail={
                "code": "storage_error", "message": f"Upload failed ({r.status_code}): {r.text[:200]}"})
        return path


async def storage_sign(path: str, expires_in: int = 3600, bucket: str = None) -> str:
    """Create a signed download URL (default 1 h) for an object."""
    bucket = bucket or DOCS_BUCKET
    url = f"{SUPABASE_STORAGE_URL}/object/sign/{bucket}/{path}"
    for attempt in (1, 2):
        headers = await _storage_headers({"Content-Type": "application/json"})
        r = await http_client.post(url, headers=headers, json={"expiresIn": expires_in})
        if r.status_code in AUTH_FAIL_CODES and attempt == 1 and AUTH_MODE == "service_account":
            await ensure_token(force=True)
            continue
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail={
                "code": "storage_error", "message": f"Signing failed ({r.status_code}): {r.text[:200]}"})
        signed = r.json().get("signedURL") or r.json().get("signedUrl") or ""
        if signed.startswith("http"):
            return signed
        if not signed.startswith("/"):
            signed = "/" + signed
        return f"{SUPABASE_STORAGE_URL}{signed}"


_MAX_MEDIA_BYTES = 10 * 1024 * 1024
_EXT_BY_TYPE = {"image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png", "image/webp": "webp",
                "image/heic": "heic", "application/pdf": "pdf", "video/mp4": "mp4",
                "audio/ogg": "ogg", "text/plain": "txt"}


async def _store_one_attachment(url: str, owner: str, entity: str) -> dict:
    """Copy one customer media URL into Storage. Never raises."""
    received_at = iso(now_local())
    url = (url or "").strip()
    if not url:
        return {"url": url, "stored": False, "content_type": None, "received_at": received_at}
    # Sales can pass a Demo Kit asset path directly (demo-assets/...): it already
    # lives in our bucket, so reference it instead of copying.
    if not url.lower().startswith(("http://", "https://")):
        ct = mimetypes.guess_type(url)[0]
        return {"path": url.lstrip("/"), "stored": url.lstrip("/").startswith("demo-assets/"),
                "content_type": ct, "received_at": received_at}
    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as c:
            async with c.stream("GET", url) as r:
                if r.status_code >= 400:
                    raise RuntimeError(f"HTTP {r.status_code}")
                buf = bytearray()
                async for chunk in r.aiter_bytes():
                    buf.extend(chunk)
                    if len(buf) > _MAX_MEDIA_BYTES:
                        raise RuntimeError("file larger than 10 MB")
                ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        if not ct or ct == "application/octet-stream":
            ct = mimetypes.guess_type(url.split("?")[0])[0] or "application/octet-stream"
        ext = _EXT_BY_TYPE.get(ct) or (mimetypes.guess_extension(ct) or ".bin").lstrip(".")
        path = f"attachments/{owner}/{entity}/{uuid.uuid4().hex}.{ext}"
        await storage_upload(path, bytes(buf), ct)
        return {"path": path, "stored": True, "content_type": ct, "received_at": received_at}
    except Exception as e:
        print(f"[media] could not copy {url[:120]}: {e}")
        return {"url": url, "stored": False, "content_type": None, "received_at": received_at}


async def store_attachments(urls: Optional[list], owner: str, entity: str) -> list:
    """Copy every attachment in parallel (§1.5). Failures -> stored=false."""
    urls = [u for u in (urls or []) if isinstance(u, str) and u.strip()]
    if not urls:
        return []
    return list(await asyncio.gather(*[_store_one_attachment(u, owner, entity) for u in urls]))


def pay_url(token: Optional[str]) -> Optional[str]:
    return f"{PORTAL_BASE_URL}/pay/{token}" if token else None


# ============================================================
# Ops: /, /health, /whoami
# ============================================================

@app.get("/")
async def root():
    return {
        "service": SERVICE_NAME,
        "version": API_VERSION,
        "multi_tenant": True,
        "auth_mode": AUTH_MODE,
        "supabase_configured": bool(SUPABASE_URL),
        "default_owner_configured": bool(DEFAULT_OWNER_ID),
        "portal_configured": bool(PORTAL_BASE_URL),
        "timezone": TZ_NAME,
        "now": iso(now_local()),
    }


@app.get("/health")
async def health():
    """Derived health (used by cron warming). Status is 'ok' only when env is
    complete, auth works and a scoped query against the fallback tenant
    succeeds. Also reports cache warmth for production diagnostics."""
    if _missing:
        return {"status": "degraded", "version": API_VERSION,
                "reason": f"missing env vars: {', '.join(_missing)}", "auth": _auth_stats()}

    checks = {"api": "ok"}
    try:
        rows = await sb_get("customers", {"select": "customer_id", "limit": "1"}, owner=DEFAULT_OWNER_ID)
        checks["supabase"] = "ok" if rows else "no_data"
    except HTTPException as e:
        checks["supabase"] = f"error: {json.dumps(e.detail)[:200]}"
    except Exception as e:
        checks["supabase"] = f"error: {str(e)[:200]}"
    try:
        rules = await get_rules()
        checks["reference_data"] = "ok" if rules else "empty"
    except Exception as e:
        checks["reference_data"] = f"error: {str(e)[:200]}"

    auth = _auth_stats()
    healthy = (checks["supabase"] in ("ok", "no_data") and checks["reference_data"] == "ok"
               and auth["authenticated"])
    return {
        "status": "ok" if healthy else "degraded",
        "version": API_VERSION,
        "multi_tenant": True,
        "auth_mode": AUTH_MODE,
        "default_owner_configured": bool(DEFAULT_OWNER_ID),
        "now": iso(now_local()),
        "checks": checks,
        "auth": auth,
        "reference_cache": _cache_stats(),
        "tenant_cache": _tenant_cache_stats(),
    }


# /whoami — tenant routing diagnostic. A broken phone->tenant lookup is
# invisible from outside: every request silently falls back to
# DEFAULT_OWNER_ID and /health still passes. This bypasses (but reports) the
# tenant cache so a stale entry can be told apart from a failing lookup.
@app.get("/whoami")
async def whoami(
    caller_phone: Optional[str] = Query(None, description="Phone number to resolve, with or without '+'"),
):
    normalized = normalize_phone(caller_phone)
    result: dict = {
        "caller_phone_received": caller_phone,
        "normalized_phone": normalized,
        "default_owner_id": DEFAULT_OWNER_ID or None,
    }
    cached = _tenant_cache.get(normalized) if normalized else None
    result["cache"] = {
        "present": bool(cached),
        "owner_id": cached["owner_id"] if cached else None,
        "age_seconds": round(_monotonic() - cached["ts"], 1) if cached else None,
    }
    if not normalized:
        result.update({"matched": False, "fell_back": True, "resolved_owner_id": DEFAULT_OWNER_ID or None,
                       "reason": "caller_phone missing or not a valid phone number"})
        return result

    rows = await _sb_raw_get("demo_users", {"whatsapp_number": f"eq.{normalized}",
                                            "select": "owner_id,email", "limit": "1"})
    if rows:
        result.update({"matched": True, "fell_back": False, "resolved_owner_id": rows[0]["owner_id"],
                       "tenant_email": rows[0].get("email")})
    else:
        # 0 visible tenants while the portal shows users = the API cannot read
        # demo_users at all (RLS), not an unregistered number.
        all_rows = await _sb_raw_get("demo_users", {"select": "owner_id"})
        result.update({
            "matched": False, "fell_back": True, "resolved_owner_id": DEFAULT_OWNER_ID or None,
            "visible_tenant_count": len(all_rows),
            "reason": (f"no demo_users row with whatsapp_number = '{normalized}' "
                       f"({len(all_rows)} tenants visible to the API) — "
                       "writes for this caller will land in the fallback tenant"),
        })
    return result


# ============================================================
# Enrichment (pure functions, no I/O)
# ============================================================

CLOSED_CLAIM_STATUSES = ("Paid", "Closed", "Rejected")
LIVE_POLICY_STATUSES = ("Active", "Pending Payment")


def vehicle_view(v: dict) -> dict:
    """Vehicle row + derived age / inspection / registration flags + labels."""
    today = today_local()
    out = _strip_one(v)
    year = v.get("year")
    out["age_years"] = (today.year - int(year)) if year else None
    insp = parse_date(v.get("inspection_valid_until"))
    reg = parse_date(v.get("registration_expiry"))
    out["inspection_valid"] = bool(insp and insp >= today)
    out["registration_valid"] = bool(reg and reg >= today)
    put_labels(out, "inspection_valid_until", v.get("inspection_valid_until"))
    put_labels(out, "registration_expiry", v.get("registration_expiry"))
    if v.get("transferred_on"):
        put_labels(out, "transferred_on", v.get("transferred_on"))
    for k in ("market_value_sar", "tpl_annual_sar", "comp_rate_percent", "open_traffic_fines_sar"):
        if k in out:
            out[k] = _num(out[k])
    return out


def vehicle_summary(v: Optional[dict]) -> Optional[dict]:
    if not v:
        return None
    today = today_local()
    return {
        "vehicle_id": v.get("vehicle_id"),
        "sequence_number": v.get("sequence_number"),
        "plate_en": v.get("plate_en"), "plate_ar": v.get("plate_ar"),
        "make_en": v.get("make_en"), "make_ar": v.get("make_ar"),
        "model_en": v.get("model_en"), "model_ar": v.get("model_ar"),
        "year": v.get("year"),
        "age_years": (today.year - int(v["year"])) if v.get("year") else None,
        "category": v.get("category"),
        "market_value_sar": _num(v.get("market_value_sar")),
        "transfer_status": v.get("transfer_status"),
    }


def member_view(m: dict, classes: dict) -> dict:
    """Health member + remaining annual / dental / optical limits (server-side
    subtraction — the agent never does arithmetic on limits)."""
    today = today_local()
    cls = classes.get(m.get("class_code")) or {}
    out = _strip_one(m)
    annual = D(cls.get("annual_limit_sar"))
    dental = D(cls.get("dental_limit_sar"))
    optical = D(cls.get("optical_limit_sar"))
    used, dused, oused = D(m.get("limit_used_sar")), D(m.get("dental_used_sar")), D(m.get("optical_used_sar"))
    out.update({
        "limit_used_sar": money(used), "dental_used_sar": money(dused), "optical_used_sar": money(oused),
        "annual_limit_sar": money(annual), "dental_limit_sar": money(dental), "optical_limit_sar": money(optical),
        "limit_remaining_sar": money(max(Decimal(0), annual - used)),
        "dental_remaining_sar": money(max(Decimal(0), dental - dused)),
        "optical_remaining_sar": money(max(Decimal(0), optical - oused)),
        "class_name_en": cls.get("name_en"), "class_name_ar": cls.get("name_ar"),
        "network_tier": cls.get("network_tier"),
        "copay_per_visit_sar": _num(cls.get("copay_per_visit_sar")),
    })
    wp = parse_date(m.get("waiting_period_until"))
    out["waiting_period_active"] = bool(wp and wp > today)
    if wp:
        put_labels(out, "waiting_period_until", m.get("waiting_period_until"))
    return out


def class_view(cls: Optional[dict]) -> Optional[dict]:
    if not cls:
        return None
    out = _strip_one(cls)
    for k in list(out):
        if k.endswith("_sar") or k.endswith("_percent"):
            out[k] = _num(out[k])
    return out


def _addon_detail(code: str, addons: dict) -> dict:
    a = addons.get(code) or {}
    return {"addon_code": code, "name_en": a.get("name_en"), "name_ar": a.get("name_ar"),
            "limit_text_en": a.get("limit_text_en"), "limit_text_ar": a.get("limit_text_ar"),
            "per_accident_limit_sar": _num(a.get("per_accident_limit_sar"))}


def enrich_policy(p: dict, ctx: dict) -> dict:
    """Policy + days_left + labels + plan names + product-specific cover."""
    today = today_local()
    out = _strip_one(p)
    out["premium_paid_sar"] = _num(p.get("premium_paid_sar"))
    end = parse_date(p.get("end_date"))
    start = parse_date(p.get("start_date"))
    out["days_left"] = max(0, (end - today).days) if end else None
    if end and end < today:
        out["ended_days_ago"] = (today - end).days
    put_labels(out, "start_date", p.get("start_date"))
    put_labels(out, "end_date", p.get("end_date"))
    out["end_date_short_en"], out["end_date_short_ar"] = short_date_labels(end)
    plan = ctx["plans"].get(p.get("plan_code")) or {}
    out["plan_name_en"] = plan.get("name_en")
    out["plan_name_ar"] = plan.get("name_ar")
    out["cover_summary_en"] = plan.get("cover_summary_en")
    out["cover_summary_ar"] = plan.get("cover_summary_ar")
    cover = dict(p.get("cover") or {})
    product = p.get("product")

    if product == "Motor":
        v = ctx["vehicles"].get(cover.get("vehicle_id"))
        out["vehicle"] = vehicle_summary(v)
        out["addons_detail"] = [_addon_detail(c, ctx["addons"]) for c in (cover.get("addons") or [])]
        out["excess_sar"] = ctx["rules"].get("motor_excess_sar")
        # Which extras could be added, and why not — so "can I have agency
        # repair?" is answered from data, never from the model's guess.
        if p.get("status") in LIVE_POLICY_STATUSES and cover.get("cover_type") == "Comprehensive":
            opts = []
            age = (today.year - int(v["year"])) if v and v.get("year") else None
            for code, a in ctx["addons"].items():
                if code == "ADDITIONAL_DRIVER":
                    continue
                present = code in (cover.get("addons") or [])
                max_age = a.get("max_vehicle_age_years")
                eligible = not present and (max_age is None or (age is not None and age <= int(max_age)))
                reason = None
                if present:
                    reason = "already_on_policy"
                elif not eligible:
                    reason = "vehicle_too_old"
                opts.append({"addon_code": code, "name_en": a.get("name_en"), "name_ar": a.get("name_ar"),
                             "annual_price_sar": _num(a.get("annual_price_sar")),
                             "max_vehicle_age_years": _num(max_age), "eligible": eligible,
                             "reason": reason})
            out["addon_options"] = opts
    elif product == "Health":
        cls = ctx["classes"].get(cover.get("class_code"))
        out["class"] = class_view(cls)
        members = ctx["members_by_policy"].get(p.get("policy_id"), [])
        out["members"] = [member_view(m, ctx["classes"]) for m in members]
        out["family_member_count"] = len([m for m in members if m.get("relationship") != "Principal"])
    elif product == "Travel":
        pd = plan.get("details") or {}
        out["is_schengen"] = "schengen" in (str(p.get("plan_code")) + str(plan.get("name_en"))).lower()
        out["plan_limits"] = pd
    elif product == "Home":
        out["excess_sar"] = _num(cover.get("excess_sar")) or ctx["rules"].get("home_excess_sar")
    out["cover"] = cover
    return out


def enrich_claim(c: dict, ctx: dict) -> dict:
    """Claim + days_open + payment_due_by (derived if missing) + overdue flag.

    overdue = payment_due_by < today AND status not Paid/Closed/Rejected.
    This is the I-27 fact: the model never compares dates itself."""
    today = today_local()
    out = _strip_one(c)
    opened = parse_ts(c.get("opened_at"))
    out["days_open"] = (today - opened.date()).days if opened else None
    put_labels(out, "opened_at", c.get("opened_at"), "datetime")
    due = parse_date(c.get("payment_due_by"))
    if not due and c.get("documents_completed_at"):
        comp = parse_ts(c.get("documents_completed_at"))
        if comp:
            due = comp.date() + timedelta(days=int(ctx["rules"].get("claim_payment_days") or 15))
            out["payment_due_by"] = due.isoformat()
    put_labels(out, "payment_due_by", due.isoformat() if due else None)
    live = c.get("status") not in CLOSED_CLAIM_STATUSES
    out["overdue"] = bool(due and due < today and live)
    out["overdue_days"] = (today - due).days if out["overdue"] else 0
    out["payment_due_in_days"] = (due - today).days if (due and live and not out["overdue"]) else None
    for k in ("customer_pays_sar", "approved_amount_sar", "fault_percent"):
        if k in out:
            out[k] = _num(out[k])
    out["documents_complete"] = not (c.get("missing_documents") or [])
    cid = c.get("claim_id")
    if ctx.get("disputes_by_claim", {}).get(cid):
        ds = []
        for d in ctx["disputes_by_claim"][cid]:
            dd = _strip_one(d)
            put_labels(dd, "due_by", d.get("due_by"))
            ds.append(dd)
        out["valuation_disputes"] = ds
    pol = ctx.get("policies_by_id", {}).get(c.get("policy_id"))
    if pol:
        out["policy_product"] = pol.get("product")
    if c.get("vehicle_id") and ctx.get("vehicles", {}).get(c["vehicle_id"]):
        out["vehicle"] = vehicle_summary(ctx["vehicles"][c["vehicle_id"]])
    return out


def enrich_booking(b: dict, refs: dict) -> dict:
    out = _strip_one(b)
    place = refs["providers"].get(b.get("provider_id")) or refs["centres"].get(b.get("centre_id")) or {}
    out["place_name_en"] = place.get("name_en")
    out["place_name_ar"] = place.get("name_ar")
    out["address_en"] = place.get("address_en")
    out["address_ar"] = place.get("address_ar")
    doc = refs["doctors"].get(b.get("doctor_id")) if b.get("doctor_id") else None
    if doc:
        out["doctor_name_en"] = doc.get("name_en")
        out["doctor_name_ar"] = doc.get("name_ar")
    put_labels(out, "start_at", b.get("start_at"), "datetime")
    if b.get("end_at"):
        put_labels(out, "end_at", b.get("end_at"), "datetime")
    out["patient_pays_sar"] = _num(b.get("patient_pays_sar"))
    return out


def enrich_preauth(pa: dict, refs: dict, members: dict) -> dict:
    out = _strip_one(pa)
    prov = refs["providers"].get(pa.get("provider_id")) or {}
    out["provider_name_en"] = prov.get("name_en")
    out["provider_name_ar"] = prov.get("name_ar")
    m = members.get(pa.get("member_id")) or {}
    out["member_name_en"] = m.get("full_name_en")
    out["member_name_ar"] = m.get("full_name_ar")
    sub = parse_ts(pa.get("submitted_at"))
    if sub:
        out["submitted_time_label_en"], out["submitted_time_label_ar"] = time_labels(sub)
        put_labels(out, "submitted_at", pa.get("submitted_at"), "datetime")
        if pa.get("status") in ("Submitted", "Under Review"):
            out["minutes_waiting"] = int((now_local() - sub).total_seconds() // 60)
    dec = parse_ts(pa.get("decided_at"))
    if dec:
        out["decided_time_label_en"], out["decided_time_label_ar"] = time_labels(dec)
        put_labels(out, "decided_at", pa.get("decided_at"), "datetime")
    out["approved_amount_sar"] = _num(pa.get("approved_amount_sar"))
    return out


def payment_view(pr: dict) -> dict:
    out = _strip_one(pr, "pay_token")
    out["amount_sar"] = _num(pr.get("amount_sar"))
    out["pay_url"] = pay_url(pr.get("pay_token")) if pr.get("status") == "Pending" else None
    put_labels(out, "created", pr.get("created_at"), "datetime")
    if pr.get("paid_at"):
        put_labels(out, "paid_at", pr.get("paid_at"), "datetime")
    return out


async def _refs() -> dict:
    """All shared catalogs keyed by id (served from the 60 s cache)."""
    providers, centres, doctors, classes, plans, addons, rules = await asyncio.gather(
        get_ref_map("providers"), get_ref_map("service_centres"), get_ref_map("provider_doctors"),
        get_ref_map("health_classes"), get_ref_map("plans"), get_ref_map("addons"), get_rules(),
    )
    return {"providers": providers, "centres": centres, "doctors": doctors, "classes": classes,
            "plans": plans, "addons": addons, "rules": rules}


async def load_customer(customer_id: Optional[str], owner: str) -> dict:
    if not customer_id:
        raise err(400, "customer_id_required", "customer_id is required")
    c = await sb_get_one("customers", {"customer_id": f"eq.{customer_id.strip()}"}, owner=owner)
    if not c:
        raise err(404, "customer_not_found", f"No customer {customer_id}")
    return c


async def load_policy(policy_id: Optional[str], owner: str, customer_id: Optional[str] = None,
                      product: Optional[str] = None) -> dict:
    if not policy_id:
        raise err(400, "policy_id_required", "policy_id is required")
    p = await sb_get_one("policies", {"policy_id": f"eq.{policy_id.strip()}"}, owner=owner)
    if not p or (customer_id and p.get("customer_id") != customer_id):
        raise err(404, "policy_not_found", f"No policy {policy_id} for this customer")
    if product and p.get("product") != product:
        raise err(409, "wrong_product", f"{policy_id} is a {p.get('product')} policy, not {product}",
                  product=p.get("product"))
    return p


# ============================================================
# READ: /customer (the workhorse — one parallel gather)
# ============================================================

@app.get("/customer")
async def get_customer(
    customer_id: Optional[str] = Query(None),
    national_id: Optional[str] = Query(None),
    policy_id: Optional[str] = Query(None),
    phone: Optional[str] = Query(None),
    caller_phone: Optional[str] = Query(None, description="Demo tenant routing — WhatsApp sender number"),
):
    """Full customer package in one call. Lookup by customer_id | national_id |
    policy_id | phone. All related reads run in ONE asyncio.gather."""
    owner = await resolve_owner(caller_phone)

    # Step 1: find the customer (within this tenant)
    customer = None
    if customer_id:
        customer = await sb_get_one("customers", {"customer_id": f"eq.{customer_id.strip()}"}, owner=owner)
    elif national_id:
        nid = re.sub(r"\D", "", national_id)
        customer = await sb_get_one("customers", {"national_id": f"eq.{nid}"}, owner=owner)
    elif policy_id:
        pol = await sb_get_one("policies", {"policy_id": f"eq.{policy_id.strip()}",
                                            "select": "customer_id"}, owner=owner)
        if pol and pol.get("customer_id"):
            customer = await sb_get_one("customers", {"customer_id": f"eq.{pol['customer_id']}"}, owner=owner)
    elif phone:
        normalized = normalize_phone(phone)
        if normalized:
            customer = await sb_get_one("customers", {"phone": f"eq.{normalized}"}, owner=owner)
        if not customer:
            customer = await sb_get_one("customers", {"phone": f"eq.{phone.strip()}"}, owner=owner)
    else:
        raise err(400, "missing_identifier", "Provide customer_id, national_id, policy_id or phone")
    if not customer:
        raise err(404, "customer_not_found", "No Watheeq customer matches that identifier")

    cid = customer["customer_id"]

    # Step 2: every related read in parallel, tenant-scoped. Shared catalogs
    # come from the in-process cache.
    (
        dependents, vehicles, policies, members, claims, disputes, preauths, bookings,
        quotes, payments, refunds, complaints, refs,
    ) = await asyncio.gather(
        sb_get("dependents", {"customer_id": f"eq.{cid}", "order": "dependent_id.asc"}, owner=owner),
        sb_get("vehicles", {"owner_customer_id": f"eq.{cid}", "order": "vehicle_id.asc"}, owner=owner),
        sb_get("policies", {"customer_id": f"eq.{cid}", "order": "end_date.desc"}, owner=owner),
        sb_get("health_members", {"customer_id": f"eq.{cid}", "order": "member_id.asc"}, owner=owner),
        sb_get("claims", {"customer_id": f"eq.{cid}", "order": "opened_at.desc"}, owner=owner),
        sb_get("valuation_disputes", {"customer_id": f"eq.{cid}", "order": "opened_at.desc"}, owner=owner),
        sb_get("preauths", {"customer_id": f"eq.{cid}", "order": "submitted_at.desc"}, owner=owner),
        sb_get("bookings", {"customer_id": f"eq.{cid}", "order": "start_at.asc"}, owner=owner),
        sb_get("quotes", {"customer_id": f"eq.{cid}", "status": "eq.Open", "order": "created_at.desc"},
               owner=owner),
        sb_get("payment_requests", {"customer_id": f"eq.{cid}", "order": "created_at.desc"}, owner=owner),
        sb_get("refunds", {"customer_id": f"eq.{cid}", "order": "created_at.desc"}, owner=owner),
        sb_get("complaints", {"customer_id": f"eq.{cid}", "order": "opened_at.desc"}, owner=owner),
        _refs(),
    )

    # Motor policies can point at vehicles the customer no longer owns (sold,
    # I-04) — include those vehicles for policy enrichment.
    vehicles_by_id = {v["vehicle_id"]: v for v in vehicles}
    missing_vids = {(p.get("cover") or {}).get("vehicle_id") for p in policies if p.get("product") == "Motor"}
    missing_vids |= {c.get("vehicle_id") for c in claims}
    missing_vids = {v for v in missing_vids if v and v not in vehicles_by_id}
    if missing_vids:
        extra = await sb_get("vehicles", {"vehicle_id": f"in.({','.join(sorted(missing_vids))})"}, owner=owner)
        for v in extra:
            vehicles_by_id[v["vehicle_id"]] = v

    today = today_local()
    now = now_local()
    rules = refs["rules"]
    members_by_policy: dict = {}
    for m in members:
        members_by_policy.setdefault(m.get("policy_id"), []).append(m)
    ctx = {
        "rules": rules, "plans": refs["plans"], "addons": refs["addons"], "classes": refs["classes"],
        "vehicles": vehicles_by_id, "members_by_policy": members_by_policy,
        "policies_by_id": {p["policy_id"]: p for p in policies},
        "disputes_by_claim": {},
    }
    for d in disputes:
        ctx["disputes_by_claim"].setdefault(d.get("claim_id"), []).append(d)

    # Step 3: enrich + segment
    pol_active, pol_expired, pol_cancelled = [], [], []
    for p in policies:
        ep = enrich_policy(p, ctx)
        st = p.get("status")
        if st in LIVE_POLICY_STATUSES:
            pol_active.append(ep)
        elif st == "Cancelled":
            pol_cancelled.append(ep)
        else:
            end = parse_date(p.get("end_date"))
            if end and (today - end).days <= 365:
                pol_expired.append(ep)
    pol_active.sort(key=lambda x: x.get("end_date") or "")

    claims_open, claims_closed = [], []
    for c in claims:
        ec = enrich_claim(c, ctx)
        (claims_closed if c.get("status") in CLOSED_CLAIM_STATUSES else claims_open).append(ec)

    members_by_id = {m["member_id"]: m for m in members}
    bk_up, bk_past = [], []
    for b in bookings:
        eb = enrich_booking(b, refs)
        st = parse_ts(b.get("start_at"))
        if b.get("status") == "Booked" and st and st >= now - timedelta(hours=2):
            bk_up.append(eb)
        else:
            bk_past.append(eb)
    bk_past.reverse()

    quotes_open = []
    for q in quotes:
        exp = parse_ts(q.get("expires_at"))
        if exp and exp > now:
            eq = _strip_one(q)
            eq["total_sar"] = _num(q.get("total_sar"))
            put_labels(eq, "expires_at", q.get("expires_at"), "datetime")
            quotes_open.append(eq)

    pay_pending = [payment_view(x) for x in payments if x.get("status") == "Pending"]
    pay_paid = [payment_view(x) for x in payments if x.get("status") == "Paid"]

    refunds_out = []
    for r in refunds:
        er = _strip_one(r)
        er["amount_sar"] = _num(r.get("amount_sar"))
        put_labels(er, "expected_by", r.get("expected_by"))
        refunds_out.append(er)

    complaints_out = []
    for c in complaints:
        ec = _strip_one(c)
        put_labels(ec, "due_by", c.get("due_by"))
        due = parse_date(c.get("due_by"))
        ec["past_due"] = bool(due and due < today and c.get("status") != "Resolved")
        complaints_out.append(ec)

    renewals = []
    for p in pol_active:
        if p.get("status") == "Active" and p.get("days_left") is not None and p["days_left"] <= 60:
            renewals.append({"policy_id": p["policy_id"], "product": p.get("product"),
                             "plan_name_en": p.get("plan_name_en"), "plan_name_ar": p.get("plan_name_ar"),
                             "end_date": p.get("end_date"), "end_date_label_en": p.get("end_date_label_en"),
                             "end_date_label_ar": p.get("end_date_label_ar"), "days_left": p["days_left"]})

    cust_out = _strip_one(customer, "national_id", "demo_notes")
    cust_out["national_id_masked"] = mask_national_id(customer.get("national_id"))
    put_labels(cust_out, "dob", customer.get("dob"))

    deps_out = []
    for d in dependents:
        dd = _strip_one(d, "national_id_or_birth_cert")
        dob = parse_date(d.get("dob"))
        dd["age_years"] = (today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))) if dob else None
        dd["age_days"] = (today - dob).days if dob else None
        deps_out.append(dd)

    return {
        "customer": cust_out,
        "rules": rules,
        "today": today.isoformat(),
        "today_label_en": date_labels(today)[0],
        "today_label_ar": date_labels(today)[1],
        "dependents": deps_out,
        "vehicles": [vehicle_view(v) for v in vehicles],
        "policies": {"active": pol_active, "expired": pol_expired, "cancelled": pol_cancelled},
        "claims": {"open": claims_open, "closed": claims_closed},
        "preauths": [enrich_preauth(pa, refs, members_by_id) for pa in preauths],
        "bookings": {"upcoming": bk_up, "past": bk_past[:10]},
        "quotes_open": quotes_open,
        "payment_requests": {"pending": pay_pending, "paid": pay_paid},
        "refunds": refunds_out,
        "complaints": complaints_out,
        "summary": {
            "active_policy_count": len([p for p in pol_active if p.get("status") == "Active"]),
            "open_claim_count": len(claims_open),
            "overdue_claim_count": len([c for c in claims_open if c.get("overdue")]),
            "pending_payments_total_sar": money(sum(D(p.get("amount_sar")) for p in pay_pending)),
            "renewals_due_60d": renewals,
        },
    }


# ============================================================
# READ: /najm — the mock Najm accident-report system (per-tenant)
# ============================================================

def normalize_najm(v: str) -> str:
    s = (v or "").strip().upper().replace(" ", "")
    if re.fullmatch(r"\d{5,10}", s):
        s = "NJM-" + s
    if re.fullmatch(r"NJM\d+", s):
        s = "NJM-" + s[3:]
    return s


def najm_view(r: dict, rules: dict) -> dict:
    out = _strip_one(r)
    parties = []
    for p in (r.get("parties") or []):
        pp = dict(p)
        pp["fault_percent"] = _num(p.get("fault_percent")) or 0
        parties.append(pp)
    out["parties"] = parties
    out["estimated_damage_sar"] = _num(r.get("estimated_damage_sar"))
    wq = next((p for p in parties if p.get("is_watheeq_customer")), None)
    out["watheeq_customer_party"] = wq.get("party") if wq else None
    at_fault = max(parties, key=lambda p: p["fault_percent"], default=None)
    out["at_fault_party"] = at_fault.get("party") if at_fault and at_fault["fault_percent"] > 0 else None
    out["at_fault_is_watheeq_customer"] = bool(at_fault and at_fault["fault_percent"] > 0
                                               and at_fault.get("is_watheeq_customer"))
    acc = parse_ts(r.get("accident_at"))
    put_labels(out, "accident_at", r.get("accident_at"), "datetime")
    if acc:
        out["accident_time_label_en"], out["accident_time_label_ar"] = time_labels(acc)
        out["hours_since_accident"] = round((now_local() - acc).total_seconds() / 3600, 1)
    return out


@app.get("/najm")
async def get_najm_report(
    report_number: str = Query(...),
    caller_phone: Optional[str] = Query(None, description="Demo tenant routing — WhatsApp sender number"),
):
    """Accident report with parties, fault split and which party (if any) is a
    Watheeq customer. Identity of the WhatsApp caller is NOT inferred here."""
    owner = await resolve_owner(caller_phone)
    num = normalize_najm(report_number)
    r = await sb_get_one("najm_reports", {"report_number": f"eq.{num}"}, owner=owner)
    if not r:
        raise err(404, "report_not_found", f"No Najm report {num}")
    rules, existing = await asyncio.gather(
        get_rules(),
        sb_get("claims", {"najm_report_number": f"eq.{num}", "select": "claim_id,claim_type,status,customer_id"},
               owner=owner),
    )
    out = najm_view(r, rules)
    out["existing_claims"] = existing
    out["rules"] = {k: rules.get(k) for k in ("najm_phone", "najm_report_window_hours",
                                              "najm_self_report_max_damage_sar", "police",
                                              "emergency_red_crescent")}
    return out


# ============================================================
# READ: /vehicle — traffic registry lookup by sequence number (I-06)
# ============================================================

@app.get("/vehicle")
async def lookup_vehicle(
    sequence_number: str = Query(...),
    caller_phone: Optional[str] = Query(None, description="Demo tenant routing — WhatsApp sender number"),
):
    """Registry row with the registered owner's name masked and an instant
    price preview (TPL = tpl_annual; Comprehensive = market value × comp rate)."""
    owner = await resolve_owner(caller_phone)
    seq = re.sub(r"\D", "", sequence_number or "")
    if not seq:
        raise err(400, "invalid_sequence_number", "sequence_number must be digits")
    v = await sb_get_one("vehicles", {"sequence_number": f"eq.{seq}"}, owner=owner)
    if not v:
        raise err(404, "vehicle_not_found", f"No vehicle with sequence number {seq}")
    vv = vehicle_view(v)
    comp = money(D(v.get("market_value_sar")) * D(v.get("comp_rate_percent")) / Decimal(100))
    return {
        "vehicle_id": v.get("vehicle_id"),
        "sequence_number": v.get("sequence_number"),
        "plate_en": v.get("plate_en"), "plate_ar": v.get("plate_ar"),
        "make_en": v.get("make_en"), "make_ar": v.get("make_ar"),
        "model_en": v.get("model_en"), "model_ar": v.get("model_ar"),
        "year": v.get("year"), "age_years": vv.get("age_years"),
        "color_en": v.get("color_en"), "color_ar": v.get("color_ar"),
        "category": v.get("category"),
        "owner_name_masked": mask_person_name(v.get("owner_name_en")),
        "transfer_status": v.get("transfer_status"),
        "insurance_status": v.get("insurance_status"),
        "registration_expiry": v.get("registration_expiry"),
        "registration_expiry_label_en": vv.get("registration_expiry_label_en"),
        "registration_expiry_label_ar": vv.get("registration_expiry_label_ar"),
        "inspection_valid": vv.get("inspection_valid"),
        "open_traffic_fines_sar": _num(v.get("open_traffic_fines_sar")),
        "quote_preview": {"tpl_sar": money(v.get("tpl_annual_sar")), "comp_sar": comp},
    }


# ============================================================
# Availability: slot generation + opaque slot_ref (§1.4 — never stored)
# ============================================================

BOOKING_TYPES = ("Inspection", "Hospital Appointment", "Rental Car", "Home Inspection", "Dental")
RENTAL_DAILY_RATE_SAR = 280           # Watheeq rental partner rate (I-11)
INSPECTION_TIMES = [(9, 0), (11, 30), (14, 0)]
HOSPITAL_TIMES = [(9, 0), (11, 0), (14, 0), (16, 30)]
DENTAL_TIMES = [(10, 0), (12, 30), (17, 0)]
HOME_WINDOWS = [((10, 0), (12, 0)), ((14, 0), (16, 0))]
SLOT_MINUTES = {"Inspection": 45, "Hospital Appointment": 30, "Dental": 45}

SPECIALTY_SYNONYMS = {
    "bone": "orthopedic", "bones": "orthopedic", "orthopaedic": "orthopedic", "orthopaedics": "orthopedic",
    "orthopedics": "orthopedic", "عظام": "orthopedic", "joint": "orthopedic",
    "heart": "cardio", "cardiology": "cardio", "قلب": "cardio",
    "skin": "dermatolog", "dermatology": "dermatolog", "جلدية": "dermatolog",
    "children": "pediatric", "kids": "pediatric", "paediatrics": "pediatric", "pediatrics": "pediatric",
    "أطفال": "pediatric",
    "eye": "ophthalm", "eyes": "ophthalm", "ophthalmology": "ophthalm", "عيون": "ophthalm",
    "women": "gyn", "obgyn": "gyn", "gynecology": "gyn", "نساء": "gyn",
    "teeth": "dent", "dental": "dent", "dentistry": "dent", "أسنان": "dent",
    "ent": "ent", "ear": "ent", "أنف": "ent",
    "internal medicine": "internal", "باطنية": "internal",
    "general": "general", "family medicine": "family",
}


def _spec_key(s: Optional[str]) -> str:
    """'Bone doctor' / 'orthopaedics' / 'عظام' -> 'orthopedic' (a substring key)."""
    k = (s or "").strip().lower()
    k = re.sub(r"\b(doctor|doctors|dr|clinic|specialist|department)\b|دكتور|طبيب|عيادة", " ", k)
    k = re.sub(r"\s+", " ", k).strip()
    return SPECIALTY_SYNONYMS.get(k, k.rstrip("s") if len(k) > 4 else k)


def split_by_tier(ranked: list, tier: int) -> tuple:
    """[(distance, provider)] -> (covered, not_covered) for a class network tier.
    A provider is covered when its min_network_tier <= the member's tier."""
    covered = [(d, p) for d, p in ranked if int(p.get("min_network_tier") or 0) <= tier]
    excluded = [(d, p) for d, p in ranked if int(p.get("min_network_tier") or 0) > tier]
    return covered, excluded


def _spec_match(wanted: str, candidate: Optional[str]) -> bool:
    if not wanted:
        return True
    w = _spec_key(wanted)
    c = (candidate or "").lower()
    return bool(w) and (w in c or c.startswith(w[:6]))


def encode_slot_ref(booking_type: str, place_id: str, start: datetime, doctor_id: Optional[str] = None) -> str:
    """Opaque token for `type|provider_or_centre|iso_start|doctor_id`."""
    raw = f"{booking_type}|{place_id}|{iso(start)}|{doctor_id or ''}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_slot_ref(ref: Optional[str]) -> Optional[dict]:
    """Inverse of encode_slot_ref. Also tolerates the raw pipe form. None on garbage."""
    if not ref:
        return None
    raw = None
    s = ref.strip()
    if "|" in s:
        raw = s
    else:
        try:
            raw = base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode()
        except Exception:
            return None
    parts = raw.split("|")
    if len(parts) != 4 or parts[0] not in BOOKING_TYPES or not parts[1]:
        return None
    start = parse_ts(parts[2])
    if not start:
        return None
    return {"booking_type": parts[0], "place_id": parts[1], "start": start, "doctor_id": parts[3] or None}


def _at(d: date, hm: tuple) -> datetime:
    return datetime.combine(d, dtime(hm[0], hm[1]), tzinfo=TZ)


def _slot_key(place_id: str, start: datetime, doctor_id: Optional[str] = None) -> tuple:
    return (place_id, doctor_id or "", start.astimezone(TZ).strftime("%Y-%m-%dT%H:%M"))


async def _taken_slots(owner: str, place_col: str, place_ids: list, start: datetime, end: datetime) -> set:
    """Keys of live bookings for these places in [start, end)."""
    if not place_ids:
        return set()
    rows = await sb_get("bookings", {
        "select": "provider_id,centre_id,doctor_id,start_at,status",
        place_col: f"in.({','.join(place_ids)})",
        "status": "eq.Booked",
        "start_at": f"gte.{iso(start)}",
        "and": f"(start_at.lt.{iso(end)})",
    }, owner=owner)
    out = set()
    for b in rows:
        st = parse_ts(b.get("start_at"))
        if st:
            pid = b.get(place_col)
            out.add(_slot_key(pid, st, b.get("doctor_id") if place_col == "provider_id" else None))
            out.add(_slot_key(pid, st, None))
    return out


def _slot_out(booking_type: str, place: dict, place_id: str, start: datetime, dist_m: float,
              end: Optional[datetime] = None, doctor: Optional[dict] = None,
              patient_pays: Optional[float] = None) -> dict:
    en, ar = dt_labels(start)
    den, dar = distance_labels(dist_m)
    s = {
        "slot_ref": encode_slot_ref(booking_type, place_id, start, doctor.get("doctor_id") if doctor else None),
        "start_at": iso(start),
        "label_en": en, "label_ar": ar,
        "place_name_en": place.get("name_en"), "place_name_ar": place.get("name_ar"),
        "distance_label_en": den, "distance_label_ar": dar,
    }
    if end:
        s["end_at"] = iso(end)
        ten, tar = time_labels(end)
        s["label_en"] = f"{en}–{ten}"
        s["label_ar"] = f"{ar}–{tar}"
    if doctor:
        s["doctor_id"] = doctor.get("doctor_id")
        s["doctor_name_en"] = doctor.get("name_en")
        s["doctor_name_ar"] = doctor.get("name_ar")
    if patient_pays is not None:
        s["patient_pays_sar"] = patient_pays
    return s


def _place_out(place: dict, id_key: str, dist_m: float) -> dict:
    den, dar = distance_labels(dist_m)
    return {
        id_key: place.get(id_key),
        "name_en": place.get("name_en"), "name_ar": place.get("name_ar"),
        "type": place.get("type"),
        "address_en": place.get("address_en"), "address_ar": place.get("address_ar"),
        "phone": place.get("phone"),
        "lat": _num(place.get("lat")), "lng": _num(place.get("lng")),
        "distance_m": int(round(dist_m)),
        "distance_label_en": den, "distance_label_ar": dar,
    }


async def _resolve_member(owner: str, member_id: Optional[str], customer_id: Optional[str]) -> Optional[dict]:
    """member_id, else the principal member of the customer's live health policy."""
    if member_id:
        m = await sb_get_one("health_members", {"member_id": f"eq.{member_id.strip()}"}, owner=owner)
        if not m:
            raise err(404, "member_not_found", f"No health member {member_id}")
        return m
    if customer_id:
        ms = await sb_get("health_members", {"customer_id": f"eq.{customer_id}", "status": "eq.Active",
                                             "order": "member_id.asc"}, owner=owner)
        principal = next((m for m in ms if m.get("relationship") == "Principal"), None)
        return principal or (ms[0] if ms else None)
    return None


def _rental_limits(claim: dict, policy: Optional[dict], addons: dict) -> dict:
    """Replacement-car benefit on the claim's policy: limit / used / remaining / max_days."""
    has = bool(policy and "REPLACEMENT_CAR" in ((policy.get("cover") or {}).get("addons") or []))
    limit = D((addons.get("REPLACEMENT_CAR") or {}).get("per_accident_limit_sar") or 2000)
    rental = claim.get("rental") or {}
    used = D(rental.get("used_sar") or 0)
    remaining = max(Decimal(0), limit - used)
    rate = D(rental.get("daily_rate") or RENTAL_DAILY_RATE_SAR)
    return {"has_benefit": has, "limit_sar": money(limit), "used_sar": money(used),
            "remaining_sar": money(remaining), "daily_rate_sar": money(rate),
            "max_days": int(remaining // rate) if rate > 0 else 0}


# ============================================================
# READ: /booking/options — dynamic availability
# ============================================================

@app.get("/booking/options")
async def get_booking_options(
    booking_type: str = Query(..., description="Inspection | Hospital Appointment | Rental Car | Home Inspection | Dental"),
    lat: Optional[float] = Query(None),
    lng: Optional[float] = Query(None),
    specialty: Optional[str] = Query(None),
    claim_id: Optional[str] = Query(None),
    member_id: Optional[str] = Query(None),
    customer_id: Optional[str] = Query(None, description="Used to find the principal member / home location when member_id or claim_id is absent"),
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD; default tomorrow"),
    caller_phone: Optional[str] = Query(None, description="Demo tenant routing — WhatsApp sender number"),
):
    """Nearest places with free slots, computed live from today + rules minus
    existing bookings. Each slot carries an opaque slot_ref for /booking/create."""
    owner = await resolve_owner(caller_phone)
    bt = next((t for t in BOOKING_TYPES if t.lower() == (booking_type or "").strip().lower()), None)
    if not bt:
        raise err(400, "invalid_booking_type", f"booking_type must be one of {', '.join(BOOKING_TYPES)}",
                  allowed=list(BOOKING_TYPES))
    today = today_local()
    now = now_local()
    start_day = parse_date(from_date) if from_date else None
    if from_date and not start_day:
        raise err(400, "invalid_date", "from_date must be YYYY-MM-DD")
    if not start_day or start_day <= today:
        start_day = today + timedelta(days=1)

    refs = await _refs()
    claim = policy = member = customer = None
    if claim_id:
        claim = await sb_get_one("claims", {"claim_id": f"eq.{claim_id.strip()}"}, owner=owner)
        if not claim:
            raise err(404, "claim_not_found", f"No claim {claim_id}")
        customer_id = customer_id or claim.get("customer_id")
        if claim.get("policy_id"):
            policy = await sb_get_one("policies", {"policy_id": f"eq.{claim['policy_id']}"}, owner=owner)
    if bt in ("Hospital Appointment", "Dental"):
        member = await _resolve_member(owner, member_id, customer_id)
        if not member:
            raise err(400, "member_required", "Provide member_id (or customer_id with an active health policy)")
        customer_id = customer_id or member.get("customer_id")
    if customer_id:
        customer = await sb_get_one("customers", {"customer_id": f"eq.{customer_id}"}, owner=owner)

    anchor = home_of(customer)
    ulat, ulng, snapped = await snap_location(lat, lng, anchor)
    result: dict = {"booking_type": bt, "location_used": {"lat": ulat, "lng": ulng},
                    "location_snapped": snapped, "options": []}

    def nearest(rows, n):
        """Nearest first; places in another city (> 60 km) only when nothing is local."""
        scored = [(haversine_m(ulat, ulng, r["lat"], r["lng"]), r) for r in rows
                  if r.get("lat") is not None and r.get("lng") is not None]
        scored.sort(key=lambda x: x[0])
        local = [x for x in scored if x[0] <= SNAP_RADIUS_M]
        scored = local or scored
        return scored[:n] if n else scored

    if bt in ("Inspection", "Home Inspection", "Rental Car"):
        ctype = {"Inspection": "Inspection Centre", "Home Inspection": "Home Inspector Team",
                 "Rental Car": "Rental Branch"}[bt]
        centres = [c for c in refs["centres"].values() if c.get("type") == ctype]
        picks = nearest(centres, 3 if bt == "Inspection" else 2)
        ids = [c["centre_id"] for _, c in picks]

        if bt == "Inspection":
            days = next_working_days(start_day, 3, include_start=True)
            taken = await _taken_slots(owner, "centre_id", ids, _at(days[0], (0, 0)),
                                       _at(days[-1], (23, 59)))
            for dist, c in picks:
                slots = []
                for d in days:
                    for hm in INSPECTION_TIMES:
                        st = _at(d, hm)
                        if st > now and _slot_key(c["centre_id"], st) not in taken:
                            slots.append(_slot_out(bt, c, c["centre_id"], st, dist,
                                                   st + timedelta(minutes=SLOT_MINUTES[bt])))
                        if len(slots) == 3:
                            break
                    if len(slots) == 3:
                        break
                result["options"].append({**_place_out(c, "centre_id", dist), "slots": slots})

        elif bt == "Home Inspection":
            days = []
            d = today + timedelta(days=1)
            while len(days) < 2:
                if d.weekday() != 4:  # no home visits on Friday
                    days.append(d)
                d += timedelta(days=1)
            taken = await _taken_slots(owner, "centre_id", ids, _at(days[0], (0, 0)), _at(days[-1], (23, 59)))
            for dist, c in picks:
                slots = []
                for d in days:
                    for (a, b) in HOME_WINDOWS:
                        st = _at(d, a)
                        if _slot_key(c["centre_id"], st) not in taken:
                            slots.append(_slot_out(bt, c, c["centre_id"], st, dist, _at(d, b)))
                result["options"].append({**_place_out(c, "centre_id", dist), "slots": slots})

        else:  # Rental Car
            if not claim:
                raise err(400, "claim_required", "Rental Car options need the claim_id")
            lim = _rental_limits(claim, policy, refs["addons"])
            if not lim["has_benefit"]:
                raise err(409, "no_rental_benefit",
                          "This policy does not include the replacement car benefit",
                          claim_id=claim["claim_id"], policy_id=claim.get("policy_id"))
            result["rental"] = lim
            starts = []
            if now < _at(today, (13, 0)):
                starts.append(_at(today, (14, 0)))
            starts.append(_at(today + timedelta(days=1), (9, 0)))
            for dist, c in picks:
                slots = [_slot_out(bt, c, c["centre_id"], st, dist) for st in starts]
                for s in slots:
                    s["daily_rate_sar"] = lim["daily_rate_sar"]
                    s["max_days"] = lim["max_days"]
                result["options"].append({**_place_out(c, "centre_id", dist), "slots": slots})
        return result

    # ---- Hospital Appointment / Dental: class-tier network filtering --------
    cls = refs["classes"].get(member.get("class_code")) or {}
    tier = int(cls.get("network_tier") or 0)
    copay = money(cls.get("copay_per_visit_sar") or 0)
    mv = member_view(member, refs["classes"])
    result["member"] = {k: mv.get(k) for k in (
        "member_id", "full_name_en", "full_name_ar", "relationship", "class_code", "class_name_en",
        "class_name_ar", "network_tier", "copay_per_visit_sar", "waiting_period_active",
        "dental_remaining_sar", "limit_remaining_sar")}

    if bt == "Hospital Appointment":
        if not specialty:
            raise err(400, "specialty_required", "Provide the specialty (e.g. Orthopedics)")

        def offers(p):
            return any(_spec_match(specialty, s) for s in (p.get("specialties") or [])) or any(
                _spec_match(specialty, d.get("specialty_en")) for d in refs["doctors"].values()
                if d.get("provider_id") == p.get("provider_id"))
        candidates = [p for p in refs["providers"].values()
                      if p.get("type") in ("Hospital", "Clinic") and offers(p)]
        ranked = nearest(candidates, 0)
        covered, not_covered = split_by_tier(ranked, tier)
        covered = covered[:3]
        max_cov = covered[-1][0] if covered else 25000.0
        # "Close but not covered" = nearer than the farthest covered option, or within 25 km.
        excluded = [(d, p) for d, p in not_covered if d <= max(max_cov, 25000.0)][:3]
        tier_names = {int(c.get("network_tier") or 0): c.get("class_code") for c in refs["classes"].values()}
        for d, p in excluded:
            need = tier_names.get(int(p.get("min_network_tier") or 0), "a higher class")
            den, dar = distance_labels(d)
            result.setdefault("excluded_nearby", []).append({
                "provider_id": p["provider_id"], "name_en": p.get("name_en"), "name_ar": p.get("name_ar"),
                "distance_label_en": den, "distance_label_ar": dar,
                "required_class": need,
                "reason_en": f"Not in the class {member.get('class_code')} network (needs class {need})",
                "reason_ar": f"غير مشمول في شبكة الفئة {member.get('class_code')} (يتطلب الفئة {need})",
            })
        result.setdefault("excluded_nearby", [])
        days = []
        d = start_day
        while len(days) < 5:
            if d.weekday() != 4:
                days.append(d)
            d += timedelta(days=1)
        ids = [p["provider_id"] for _, p in covered]
        taken = await _taken_slots(owner, "provider_id", ids, _at(days[0], (0, 0)), _at(days[-1], (23, 59)))
        for idx, (dist, p) in enumerate(covered):
            docs = [x for x in refs["doctors"].values() if x.get("provider_id") == p["provider_id"]
                    and _spec_match(specialty, x.get("specialty_en"))]
            docs.sort(key=lambda x: x.get("doctor_id"))
            slots = []
            # Deterministic spread so neighbouring hospitals don't all offer
            # the same hour: offset the grid by provider position.
            grid = [(dd, hm) for dd in days for hm in HOSPITAL_TIMES]
            for k, (dd, hm) in enumerate(grid):
                if (k + idx * 3) % 5 != 1:
                    continue
                st = _at(dd, hm)
                doc = docs[len(slots) % len(docs)] if docs else None
                if st <= now or _slot_key(p["provider_id"], st, doc.get("doctor_id") if doc else None) in taken:
                    continue
                slots.append(_slot_out(bt, p, p["provider_id"], st, dist,
                                       st + timedelta(minutes=SLOT_MINUTES[bt]), doc, copay))
                if len(slots) == 2:
                    break
            result["options"].append({**_place_out(p, "provider_id", dist), "patient_pays_sar": copay,
                                      "slots": slots})
        return result

    # Dental — cleanings covered from the dental sub-limit; cosmetic never.
    rules = refs["rules"]
    result["dental"] = {"dental_remaining_sar": mv.get("dental_remaining_sar"),
                        "dental_limit_sar": mv.get("dental_limit_sar"),
                        "cleanings_per_year": rules.get("dental_cleanings_per_year"),
                        "cosmetic_covered": bool(rules.get("cosmetic_dental_covered"))}
    candidates = [p for p in refs["providers"].values()
                  if (p.get("type") == "Dental Clinic" or any(_spec_match("dental", s) for s in (p.get("specialties") or [])))
                  and int(p.get("min_network_tier") or 0) <= tier]
    picks = nearest(candidates, 3)
    days = []
    d = start_day
    while len(days) < 5:
        if d.weekday() != 4:
            days.append(d)
        d += timedelta(days=1)
    ids = [p["provider_id"] for _, p in picks]
    taken = await _taken_slots(owner, "provider_id", ids, _at(days[0], (0, 0)), _at(days[-1], (23, 59)))
    for idx, (dist, p) in enumerate(picks):
        docs = [x for x in refs["doctors"].values() if x.get("provider_id") == p["provider_id"]]
        slots = []
        for k, (dd, hm) in enumerate([(dd, hm) for dd in days for hm in DENTAL_TIMES]):
            if (k + idx) % 3 != 0:
                continue
            st = _at(dd, hm)
            doc = docs[0] if docs else None
            if st <= now or _slot_key(p["provider_id"], st, doc.get("doctor_id") if doc else None) in taken:
                continue
            slots.append(_slot_out(bt, p, p["provider_id"], st, dist,
                                   st + timedelta(minutes=SLOT_MINUTES[bt]), doc, copay))
            if len(slots) == 2:
                break
        result["options"].append({**_place_out(p, "provider_id", dist), "patient_pays_sar": copay, "slots": slots})
    return result


# ============================================================
# WRITE: /booking/create
# ============================================================

@app.post("/booking/create")
async def create_booking(
    booking_type: Optional[str] = Body(None),
    slot_ref: str = Body(...),
    claim_id: Optional[str] = Body(None),
    member_id: Optional[str] = Body(None),
    customer_id: Optional[str] = Body(None),
    days: Optional[int] = Body(None, description="Rental Car only"),
    caller_phone: Optional[str] = Body(None),
):
    """Book a slot from /booking/options. Re-validates everything the options
    call promised: slot still free, class tier, rental benefit and limit."""
    owner = await resolve_owner(caller_phone)
    slot = decode_slot_ref(slot_ref)
    if not slot:
        raise err(400, "invalid_slot_ref", "slot_ref is not valid — fetch options again")
    bt = slot["booking_type"]
    if booking_type and booking_type.strip().lower() != bt.lower():
        raise err(400, "slot_type_mismatch", f"slot_ref is for {bt}, not {booking_type}", slot_booking_type=bt)
    now = now_local()
    start = slot["start"]
    if start < now - timedelta(minutes=10):
        raise err(409, "slot_in_past", "That time has already passed — fetch options again")

    refs = await _refs()
    is_provider = bt in ("Hospital Appointment", "Dental")
    place = (refs["providers"] if is_provider else refs["centres"]).get(slot["place_id"])
    if not place:
        raise err(404, "place_not_found", f"Unknown provider/centre {slot['place_id']}")

    claim = policy = member = None
    if claim_id:
        claim = await sb_get_one("claims", {"claim_id": f"eq.{claim_id.strip()}"}, owner=owner)
        if not claim:
            raise err(404, "claim_not_found", f"No claim {claim_id}")
        if claim.get("status") in CLOSED_CLAIM_STATUSES:
            raise err(409, "claim_closed", f"Claim {claim['claim_id']} is {claim.get('status')}",
                      status=claim.get("status"))
        customer_id = claim.get("customer_id") or customer_id
        if claim.get("policy_id"):
            policy = await sb_get_one("policies", {"policy_id": f"eq.{claim['policy_id']}"}, owner=owner)

    doctor = refs["doctors"].get(slot["doctor_id"]) if slot["doctor_id"] else None
    end = None
    details: dict = {}
    patient_pays = None
    rental_out = None
    cls = None

    if bt == "Rental Car":
        if not claim:
            raise err(400, "claim_required", "A rental car is booked against a claim — pass claim_id")
        lim = _rental_limits(claim, policy, refs["addons"])
        if not lim["has_benefit"]:
            raise err(409, "no_rental_benefit", "This policy does not include the replacement car benefit",
                      claim_id=claim["claim_id"], policy_id=claim.get("policy_id"))
        if not days or int(days) < 1 or int(days) > 60:
            raise err(400, "invalid_days", "days must be between 1 and 60")
        days = int(days)
        total = D(lim["daily_rate_sar"]) * days
        if total > D(lim["remaining_sar"]):
            raise err(409, "rental_limit_exceeded",
                      f"{days} days × SAR {fmt_sar(lim['daily_rate_sar'])} = SAR {fmt_sar(total)} exceeds the "
                      f"remaining SAR {fmt_sar(lim['remaining_sar'])} of the SAR {fmt_sar(lim['limit_sar'])} limit",
                      max_days=lim["max_days"], limit_sar=lim["limit_sar"], used_sar=lim["used_sar"],
                      remaining_sar=lim["remaining_sar"], daily_rate_sar=lim["daily_rate_sar"],
                      requested_days=days)
        end = start + timedelta(days=days)
        details = {"days": days, "daily_rate_sar": lim["daily_rate_sar"], "total_sar": money(total),
                   "pickup_branch": place.get("name_en"), "pickup_branch_ar": place.get("name_ar"),
                   "partner_name": place.get("partner_name")}
        rental_out = {"days": days, "daily_rate_sar": lim["daily_rate_sar"], "total_sar": money(total),
                      "limit_sar": lim["limit_sar"],
                      "remaining_after_sar": money(D(lim["remaining_sar"]) - total)}
    else:
        # Slot must still be free (someone may have taken it since /options).
        place_col = "provider_id" if is_provider else "centre_id"
        taken = await _taken_slots(owner, place_col, [slot["place_id"]], start - timedelta(minutes=1),
                                   start + timedelta(minutes=1))
        if _slot_key(slot["place_id"], start, slot["doctor_id"] if is_provider else None) in taken:
            raise err(409, "slot_taken", "That time was just taken — here are fresh options",
                      place_id=slot["place_id"], start_at=iso(start))
        if bt == "Home Inspection":
            # windows are 2 h (10–12, 14–16)
            end = start + timedelta(hours=2)
        else:
            end = start + timedelta(minutes=SLOT_MINUTES.get(bt, 30))

    if is_provider:
        member = await _resolve_member(owner, member_id, customer_id)
        if not member:
            raise err(400, "member_required", "Provide member_id (or customer_id with an active health policy)")
        customer_id = member.get("customer_id") or customer_id
        cls = refs["classes"].get(member.get("class_code")) or {}
        tier = int(cls.get("network_tier") or 0)
        need = int(place.get("min_network_tier") or 0)
        if need > tier:
            raise err(409, "not_covered_by_class",
                      f"{place.get('name_en')} is not in the class {member.get('class_code')} network",
                      class_code=member.get("class_code"), network_tier=tier, required_tier=need,
                      provider_id=place.get("provider_id"))
        patient_pays = money(cls.get("copay_per_visit_sar") or 0)
        if bt == "Dental":
            mv = member_view(member, refs["classes"])
            if mv["dental_remaining_sar"] <= 0:
                raise err(409, "dental_limit_exhausted", "The dental limit for this year is used up",
                          dental_limit_sar=mv["dental_limit_sar"])
            details["service_en"] = "Dental cleaning"
            details["service_ar"] = "تنظيف أسنان"

    # A third-party claimant (I-13) has no customer row: the booking is stored
    # with customer_id NULL and linked through claim_id only, so it never lands
    # in the at-fault policyholder's /customer view.
    third_party = bool(claim and claim.get("claim_type") == "Third Party" and not claim.get("customer_id"))
    if third_party:
        customer_id = None
        details["claimant_name_en"] = (claim.get("claimant") or {}).get("name_en")
    elif not customer_id:
        raise err(400, "customer_required", "Pass customer_id, claim_id or member_id so the booking has an owner")

    booking_id = await next_id("bookings", "booking_id", "BKG-", owner)
    row = {
        "booking_id": booking_id,
        "customer_id": customer_id,
        "booking_type": bt,
        "claim_id": claim["claim_id"] if claim else None,
        "provider_id": slot["place_id"] if is_provider else None,
        "centre_id": None if is_provider else slot["place_id"],
        "doctor_id": slot["doctor_id"],
        "member_id": member["member_id"] if member else None,
        "start_at": iso(start),
        "end_at": iso(end) if end else None,
        "status": "Booked",
        "details": details,
        "patient_pays_sar": patient_pays if patient_pays is not None else 0,
    }
    await sb_insert("bookings", row, owner=owner)

    claim_status = claim.get("status") if claim else None
    if claim:
        patch = {}
        if bt in ("Inspection", "Home Inspection") and claim.get("status") in ("Open",):
            patch["status"] = "Inspection Booked"
        if bt == "Rental Car":
            prev = claim.get("rental") or {}
            patch["rental"] = {
                "booking_id": booking_id,
                "daily_rate": details["daily_rate_sar"],
                "days": int(prev.get("days") or 0) + details["days"],
                "limit_sar": rental_out["limit_sar"],
                "used_sar": money(D(prev.get("used_sar") or 0) + D(details["total_sar"])),
            }
        if patch:
            await sb_update("claims", {"claim_id": f"eq.{claim['claim_id']}"}, patch, owner=owner)
            claim_status = patch.get("status", claim_status)

    sen, sar = dt_labels(start)
    desc = f"Booked {bt} at {place.get('name_en')} — {sen}"
    if doctor:
        desc += f" with {doctor.get('name_en')}"
    if bt == "Rental Car":
        desc += f" for {details['days']} days (SAR {fmt_sar(details['total_sar'])})"
    if claim:
        desc += f" for claim {claim['claim_id']}"
    await log_agent_action(customer_id, "Booking Created", desc, {
        "booking_id": booking_id, "booking_type": bt, "claim_id": row["claim_id"],
        "provider_id": row["provider_id"], "centre_id": row["centre_id"], "member_id": row["member_id"],
        "start_at": row["start_at"],
    }, owner=owner, reference_id=booking_id)

    documents = [{"type": "rental_voucher" if bt == "Rental Car" else "booking_confirmation", "ref": booking_id}]
    if bt == "Inspection" and claim and claim.get("najm_report_number") and claim.get("customer_id"):
        documents.append({"type": "accident_pack", "ref": claim["claim_id"]})

    out = {
        "ok": True,
        "booking_id": booking_id,
        "booking_type": bt,
        "start_at": row["start_at"],
        "start_label_en": sen, "start_label_ar": sar,
        "end_at": row["end_at"],
        "place_name_en": place.get("name_en"), "place_name_ar": place.get("name_ar"),
        "address_en": place.get("address_en"), "address_ar": place.get("address_ar"),
        "phone": place.get("phone"),
        "lat": _num(place.get("lat")), "lng": _num(place.get("lng")),
        "claim_id": row["claim_id"], "claim_status": claim_status,
        "documents": documents,
    }
    if end:
        out["end_label_en"], out["end_label_ar"] = dt_labels(end)
    if doctor:
        out["doctor_name_en"] = doctor.get("name_en")
        out["doctor_name_ar"] = doctor.get("name_ar")
    if patient_pays is not None:
        out["patient_pays_sar"] = patient_pays
    if rental_out:
        out["rental"] = rental_out
    if member:
        out["member_id"] = member["member_id"]
        out["member_name_en"] = member.get("full_name_en")
    return out


# ============================================================
# Quotes — every price computed here (§3.5 formulas)
# ============================================================
# A quote is persisted with its breakdown and the exact parameters /quote/apply
# will execute. Totals are Decimal, rounded half-up to 2 dp once, at the end.

QUOTE_TYPES = ("renew", "upgrade_cover", "add_driver", "add_addon", "cancel_refund",
               "new_policy_motor", "health_class_upgrade", "add_member", "travel")

# Relationship vocabulary the customer actually uses -> the canonical values in
# business_rules.allowed_driver_relationships. Anything not mapped stays as
# typed (e.g. "Friend") and is rejected with driver_not_eligible.
DRIVER_RELATIONSHIP_SYNONYMS = {
    "spouse": "Spouse", "wife": "Spouse", "husband": "Spouse", "زوجة": "Spouse", "زوج": "Spouse",
    "parent": "Parent", "father": "Parent", "mother": "Parent", "أب": "Parent", "أم": "Parent",
    "الأب": "Parent", "الأم": "Parent", "ابوي": "Parent", "امي": "Parent",
    "child": "Child", "son": "Child", "daughter": "Child", "ابن": "Child", "بنت": "Child", "ولد": "Child",
    "sibling": "Sibling", "brother": "Sibling", "sister": "Sibling", "أخ": "Sibling", "أخت": "Sibling",
    "اخوي": "Sibling", "اختي": "Sibling", "أخوي": "Sibling",
    "employee": "Employee (work contract)", "employee (work contract)": "Employee (work contract)",
    "driver": "Employee (work contract)", "private driver": "Employee (work contract)",
    "سائق": "Employee (work contract)", "موظف": "Employee (work contract)",
}

MEMBER_RELATIONSHIP_SYNONYMS = {
    "son": "Son", "baby boy": "Son", "boy": "Son", "ابن": "Son", "ولد": "Son", "مولود": "Son",
    "daughter": "Daughter", "baby girl": "Daughter", "girl": "Daughter", "بنت": "Daughter", "مولودة": "Daughter",
    "spouse": "Spouse", "wife": "Spouse", "husband": "Spouse", "زوجة": "Spouse", "زوج": "Spouse",
    "parent": "Parent", "father": "Parent", "mother": "Parent", "أب": "Parent", "أم": "Parent",
    "sibling": "Sibling", "brother": "Sibling", "sister": "Sibling", "أخ": "Sibling", "أخت": "Sibling",
}

MEMBER_WAITING_PERIOD_DAYS = 90


def _line(label_en: str, label_ar: str, amount) -> dict:
    return {"label_en": label_en, "label_ar": label_ar, "amount": money(amount)}


def _cover_type(v: Optional[str], default: Optional[str] = None) -> Optional[str]:
    s = (v or "").strip().lower()
    if not s:
        return default
    if s in ("tpl", "third party", "third-party", "ضد الغير", "third_party"):
        return "TPL"
    if s in ("comprehensive", "comp", "full", "full cover", "شامل"):
        return "Comprehensive"
    return None


def _one_year_end(start: date) -> date:
    try:
        return start.replace(year=start.year + 1) - timedelta(days=1)
    except ValueError:  # 29 Feb
        return start + timedelta(days=364)


def _days_left(policy: dict) -> int:
    end = parse_date(policy.get("end_date"))
    return max(0, (end - today_local()).days) if end else 0


def _require_live(policy: dict):
    if policy.get("status") != "Active":
        raise err(409, "policy_not_active", f"{policy['policy_id']} is {policy.get('status')}",
                  policy_id=policy["policy_id"], status=policy.get("status"))


async def _q_renew(customer, policy, vehicle, refs, params, owner) -> dict:
    if policy.get("product") not in ("Motor", "Home"):
        raise err(409, "not_renewable_product", "Renewal quotes cover motor and home policies",
                  product=policy.get("product"))
    if policy.get("status") == "Cancelled":
        raise err(409, "not_renewable", f"{policy['policy_id']} is cancelled")
    today = today_local()
    old_end = parse_date(policy.get("end_date"))
    new_start = old_end + timedelta(days=1) if old_end and old_end >= today else today
    new_end = _one_year_end(new_start)
    cover = policy.get("cover") or {}
    offer = cover.get("renewal_offer") or {}
    rules = refs["rules"]
    out: dict = {"new_start": new_start.isoformat(), "new_end": new_end.isoformat()}
    s_en, s_ar = short_date_labels(new_start), short_date_labels(new_end, True)

    if policy["product"] == "Motor":
        ct = _cover_type(params.get("cover_type"), cover.get("cover_type") or "Comprehensive")
        if not ct:
            raise err(400, "invalid_cover_type", "cover_type must be TPL or Comprehensive")
        ncd = int(offer.get("ncd_percent") if offer.get("ncd_percent") is not None
                  else customer.get("ncd_percent") or 0)
        if ct == "Comprehensive":
            amount = D(offer["comp_sar"]) if offer.get("comp_sar") is not None else (
                D(vehicle.get("market_value_sar")) * D(vehicle.get("comp_rate_percent")) / 100 * (100 - ncd) / 100)
            lbl = (f"Comprehensive cover, 12 months ({ncd}% no-claims discount applied)",
                   f"تأمين شامل لمدة 12 شهراً (بعد خصم عدم المطالبات {ncd}%)")
        else:
            amount = D(offer["tpl_sar"]) if offer.get("tpl_sar") is not None else (
                D(vehicle.get("tpl_annual_sar")) * (100 - ncd) / 100)
            lbl = (f"Third-party cover, 12 months ({ncd}% no-claims discount applied)",
                   f"تأمين ضد الغير لمدة 12 شهراً (بعد خصم عدم المطالبات {ncd}%)")
        vv = vehicle_view(vehicle) if vehicle else {}
        fines = _num((vehicle or {}).get("open_traffic_fines_sar")) or 0
        out["registration_check"] = {
            "inspection_valid": vv.get("inspection_valid"),
            "inspection_valid_until": (vehicle or {}).get("inspection_valid_until"),
            "inspection_valid_until_label_en": vv.get("inspection_valid_until_label_en"),
            "inspection_valid_until_label_ar": vv.get("inspection_valid_until_label_ar"),
            "fines_sar": fines,
            "registration_renewal_blocked": bool((vehicle or {}).get("registration_renewal_blocked")),
            "blocked_reason_en": (vehicle or {}).get("block_reason_en"),
            "blocked_reason_ar": (vehicle or {}).get("block_reason_ar"),
            "insurance_is_only_blocker": bool(vv.get("inspection_valid")) and not fines,
        }
        out["cover_type"] = ct
        out["ncd_percent"] = ncd
        # Both prices, so "what would third-party cost?" needs no second call.
        out["alternatives"] = {"tpl_sar": _num(offer.get("tpl_sar")), "comp_sar": _num(offer.get("comp_sar"))}
        total = money(amount)
        plan = "MOTOR_COMP" if ct == "Comprehensive" else "MOTOR_TPL"
        summary = (f"Renew {ct} cover on {vehicle.get('make_en')} {vehicle.get('model_en')} "
                   f"({vehicle.get('plate_en')}) for 12 months from {s_en[0]}: SAR {fmt_sar(total)}.",
                   f"تجديد التأمين {'الشامل' if ct == 'Comprehensive' else 'ضد الغير'} على "
                   f"{vehicle.get('make_ar')} {vehicle.get('model_ar')} لمدة 12 شهراً من {s_ar[0]}: "
                   f"{fmt_sar(total)} ريال.")
        stored = {"product": "Motor", "cover_type": ct, "plan_code": plan}
    else:
        amount = D(offer.get("premium_sar") if offer.get("premium_sar") is not None else policy.get("premium_paid_sar"))
        ncd = bool(offer.get("no_claims_discount"))
        lbl = ("Home Standard, 12 months" + (" (no-claims discount kept)" if ncd else ""),
               "تأمين المنزل القياسي لمدة 12 شهراً" + (" (مع الاحتفاظ بخصم عدم المطالبات)" if ncd else ""))
        total = money(amount)
        out["no_claims_discount"] = ncd
        summary = (f"Renew home insurance for 12 months from {s_en[0]}: SAR {fmt_sar(total)}.",
                   f"تجديد تأمين المنزل لمدة 12 شهراً من {s_ar[0]}: {fmt_sar(total)} ريال.")
        stored = {"product": "Home", "plan_code": policy.get("plan_code")}
    put_labels(out, "new_start", out["new_start"])
    put_labels(out, "new_end", out["new_end"])
    return {"total": total, "breakdown": [_line(lbl[0], lbl[1], total)], "summary": summary,
            "params": {**stored, "new_start": out["new_start"], "new_end": out["new_end"]}, "extra": out}


async def _q_upgrade_cover(customer, policy, vehicle, refs, params, owner) -> dict:
    if policy.get("product") != "Motor":
        raise err(409, "wrong_product", "Cover upgrades apply to motor policies", product=policy.get("product"))
    _require_live(policy)
    cover = policy.get("cover") or {}
    if cover.get("cover_type") == "Comprehensive":
        raise err(409, "already_comprehensive", f"{policy['policy_id']} is already comprehensive")
    days = _days_left(policy)
    comp_annual = D(vehicle.get("market_value_sar")) * D(vehicle.get("comp_rate_percent")) / 100
    tpl_annual = D(vehicle.get("tpl_annual_sar"))
    diff = comp_annual - tpl_annual
    total = prorata(diff, days)
    eff = datetime.combine(today_local() + timedelta(days=1), dtime(0, 0), tzinfo=TZ)
    months = max(1, round(days / 30.4))
    breakdown = [
        _line("Comprehensive annual premium", "القسط السنوي للتأمين الشامل", comp_annual),
        _line("Less third-party annual premium", "ناقص القسط السنوي للتأمين ضد الغير", -tpl_annual),
        _line(f"Difference for the {days} days left (× {days}/365)",
              f"الفرق عن {days} يوماً المتبقية (× {days}/365)", total),
    ]
    return {"total": total, "breakdown": breakdown,
            "summary": (f"{months} months left. Moving to full cover for those months costs SAR {fmt_sar(total)}; "
                        f"full cover starts tonight at midnight.",
                        f"متبقٍ {months} أشهر. الفرق للتحويل إلى التأمين الشامل لهذه المدة {fmt_sar(total)} ريال، "
                        f"ويبدأ الشامل الليلة عند منتصف الليل."),
            "params": {"days_left": days, "effective_from": iso(eff)},
            "extra": {"days_left": days, "months_left": months,
                      "comp_annual_sar": money(comp_annual), "tpl_annual_sar": money(tpl_annual),
                      "annual_difference_sar": money(diff),
                      "formula_en": f"({fmt_sar(comp_annual)} − {fmt_sar(tpl_annual)}) × {days}/365 = SAR {fmt_sar(total)}",
                      "effective_from": iso(eff),
                      "effective_from_label_en": f"Tonight at midnight ({date_labels(eff.date())[0]}, 12:00 AM)",
                      "effective_from_label_ar": f"الليلة عند منتصف الليل ({date_labels(eff.date())[1]}، 12:00 ص)"}}


async def _q_add_driver(customer, policy, vehicle, refs, params, owner) -> dict:
    if policy.get("product") != "Motor":
        raise err(409, "wrong_product", "Drivers are added to motor policies", product=policy.get("product"))
    _require_live(policy)
    rules = refs["rules"]
    allowed = [x.strip() for x in str(rules.get("allowed_driver_relationships") or "").split(",") if x.strip()]
    name = (params.get("name_en") or params.get("full_name_en") or "").strip()
    rel_raw = (params.get("relationship") or "").strip()
    nid = re.sub(r"\D", "", str(params.get("national_id") or ""))
    lic = params.get("licence_issue_date") or params.get("license_issue_date")
    if not name or not rel_raw:
        raise err(400, "missing_fields", "name_en and relationship are required",
                  required=["name_en", "national_id", "licence_issue_date", "relationship"])
    rel = DRIVER_RELATIONSHIP_SYNONYMS.get(rel_raw.lower(), rel_raw)
    if rel not in allowed:
        raise err(409, "driver_not_eligible",
                  f"A {rel_raw} cannot be added — allowed: {', '.join(allowed)}",
                  relationship=rel_raw, allowed=allowed)
    if not valid_national_id(nid):
        raise err(400, "invalid_national_id", "National ID / iqama must be 10 digits starting with 1 or 2")
    lic_d = parse_date(lic)
    if not lic_d or lic_d > today_local():
        raise err(400, "invalid_date", "licence_issue_date must be a past date (YYYY-MM-DD)")
    drivers = (policy.get("cover") or {}).get("drivers") or []
    if any(d.get("id_last4") == nid[-4:] and _name_tokens(d.get("name_en") or "") == _name_tokens(name)
           for d in drivers):
        raise err(409, "driver_exists", f"{name} is already on the policy")
    days = _days_left(policy)
    annual = D((refs["addons"].get("ADDITIONAL_DRIVER") or {}).get("annual_price_sar") or 450)
    total = prorata(annual, days)
    driver = {"name_en": name, "name_ar": params.get("name_ar"), "relationship": rel,
              "id_last4": nid[-4:], "licence_issue_date": lic_d.isoformat()}
    return {"total": total,
            "breakdown": [_line("Additional named driver, annual price", "سائق إضافي، السعر السنوي", annual),
                          _line(f"For the {days} days left (× {days}/365)", f"عن {days} يوماً المتبقية (× {days}/365)", total)],
            "summary": (f"{name} ({rel}) can be added. SAR {fmt_sar(total)} for the rest of the policy year; covered from now.",
                        f"يمكن إضافة {name} ({rel}). {fmt_sar(total)} ريال لبقية مدة الوثيقة، ويُغطّى من الآن."),
            "params": {"driver": driver, "days_left": days},
            "extra": {"relationship_canonical": rel, "days_left": days, "eligible": True}}


async def _q_add_addon(customer, policy, vehicle, refs, params, owner) -> dict:
    if policy.get("product") != "Motor":
        raise err(409, "wrong_product", "Add-ons apply to motor policies", product=policy.get("product"))
    _require_live(policy)
    cover = policy.get("cover") or {}
    codes = params.get("addons") or params.get("addon_codes") or params.get("addon_code") or []
    if isinstance(codes, str):
        codes = [codes]
    codes = [str(c).strip().upper().replace(" ", "_") for c in codes if str(c).strip()]
    if not codes:
        raise err(400, "missing_fields", "params.addons is required (e.g. [\"AGENCY_REPAIR\"])")
    age = (today_local().year - int(vehicle["year"])) if vehicle and vehicle.get("year") else None
    days = _days_left(policy)
    lines, total = [], Decimal(0)
    per = {}
    for code in dict.fromkeys(codes):
        a = refs["addons"].get(code)
        if not a:
            raise err(400, "invalid_addon", f"Unknown add-on {code}", allowed=sorted(refs["addons"]))
        if code == "ADDITIONAL_DRIVER":
            raise err(400, "use_add_driver", "Drivers are added with quote_type add_driver")
        if cover.get("cover_type") != "Comprehensive":
            raise err(409, "addon_not_eligible", f"{a.get('name_en')} needs comprehensive cover",
                      addon_code=code, reason="requires_comprehensive")
        if code in (cover.get("addons") or []):
            raise err(409, "addon_exists", f"{a.get('name_en')} is already on the policy", addon_code=code)
        max_age = a.get("max_vehicle_age_years")
        if max_age is not None and (age is None or age > int(max_age)):
            raise err(409, "addon_not_eligible",
                      f"{a.get('name_en')} is only for cars up to {int(max_age)} years old",
                      addon_code=code, vehicle_age_years=age, max_vehicle_age_years=int(max_age),
                      reason="vehicle_too_old")
        price = prorata(a.get("annual_price_sar"), days)
        per[code] = price
        total += D(price)
        lines.append(_line(f"{a.get('name_en')} — SAR {fmt_sar(a.get('annual_price_sar'))}/year × {days}/365",
                           f"{a.get('name_ar')} — {fmt_sar(a.get('annual_price_sar'))} ريال سنوياً × {days}/365",
                           price))
    total = money(total)
    names_en = " and ".join(refs["addons"][c]["name_en"] for c in per)
    names_ar = " و".join(refs["addons"][c]["name_ar"] for c in per)
    return {"total": total, "breakdown": lines,
            "summary": (f"{names_en}: SAR {fmt_sar(total)} for the rest of the policy year, starting now.",
                        f"{names_ar}: {fmt_sar(total)} ريال لبقية مدة الوثيقة، تبدأ من الآن."),
            "params": {"addons": list(per), "prices": per, "days_left": days},
            "extra": {"days_left": days, "vehicle_age_years": age, "prices": per,
                      "limits": {c: {"limit_text_en": refs["addons"][c].get("limit_text_en"),
                                     "limit_text_ar": refs["addons"][c].get("limit_text_ar"),
                                     "per_accident_limit_sar": _num(refs["addons"][c].get("per_accident_limit_sar"))}
                                 for c in per}}}


async def _q_cancel_refund(customer, policy, vehicle, refs, params, owner) -> dict:
    if policy.get("product") != "Motor":
        raise err(409, "wrong_product", "Sale refunds apply to motor policies", product=policy.get("product"))
    if policy.get("status") == "Cancelled":
        raise err(409, "already_cancelled", f"{policy['policy_id']} is already cancelled")
    _require_live(policy)
    if not vehicle or vehicle.get("transfer_status") != "Transferred" or not vehicle.get("transferred_on"):
        raise err(409, "vehicle_not_transferred",
                  "The traffic system does not show the car as transferred yet — the refund starts from the transfer date",
                  transfer_status=(vehicle or {}).get("transfer_status"))
    rules = refs["rules"]
    transferred = parse_date(vehicle["transferred_on"])
    end = parse_date(policy.get("end_date"))
    days = max(0, min(365, (end - transferred).days)) if end and transferred else 0
    fee = D(rules.get("cancellation_fee_sar") or 30)
    premium = D(policy.get("premium_paid_sar"))
    claims_paid = int((policy.get("cover") or {}).get("claims_paid_count") or 0)
    base = premium - fee
    if claims_paid > 0:
        refund = Decimal(0)
        reason = ("A claim was paid on this policy, so no refund is due.",
                  "صُرفت مطالبة على هذه الوثيقة، لذلك لا يستحق استرداد.")
    else:
        refund = D(money(base * days / Decimal(365)))
        reason = None
    t_en, t_ar = short_date_labels(transferred)
    breakdown = [
        _line("Premium paid", "القسط المدفوع", premium),
        _line("Less cancellation fee", "ناقص رسوم الإلغاء", -fee),
        _line(f"Days left from transfer ({t_en}): {days} of 365", f"الأيام المتبقية من تاريخ النقل ({t_ar}): {days} من 365",
              base * days / Decimal(365) if not claims_paid else 0),
        _line("Refund", "المبلغ المسترد", refund),
    ]
    formula = f"({days} ÷ 365) × {fmt_sar(base)} = SAR {fmt_sar(refund)}"
    return {"total": -money(refund), "breakdown": breakdown,
            "summary": (f"Car transferred on {t_en}. {days} days left out of 365, you paid SAR {fmt_sar(premium)}, "
                        f"minus the SAR {fmt_sar(fee)} fee: {formula}." + (" " + reason[0] if reason else ""),
                        f"نُقلت ملكية السيارة في {t_ar}. متبقٍ {days} يوماً من 365، دفعت {fmt_sar(premium)} ريال "
                        f"ناقص رسوم {fmt_sar(fee)} ريال: ({days} ÷ 365) × {fmt_sar(base)} = {fmt_sar(refund)} ريال."
                        + (" " + reason[1] if reason else "")),
            "params": {"refund_sar": money(refund), "days_left_from_transfer": days},
            "extra": {"refund_sar": money(refund), "days_left_from_transfer": days,
                      "transferred_on": transferred.isoformat(), "transferred_on_label_en": t_en,
                      "transferred_on_label_ar": t_ar, "premium_paid_sar": money(premium),
                      "cancellation_fee_sar": money(fee), "claims_paid_count": claims_paid,
                      "no_refund_reason_en": reason[0] if reason else None,
                      "no_refund_reason_ar": reason[1] if reason else None,
                      "formula_en": formula, "requires_iban": True,
                      "iban_on_file": customer.get("iban_masked")}}


async def _q_new_policy_motor(customer, policy, vehicle, refs, params, owner) -> dict:
    if not vehicle:
        raise err(400, "missing_fields", "sequence_number is required")
    ct = _cover_type(params.get("cover_type"), "Comprehensive")
    if not ct:
        raise err(400, "invalid_cover_type", "cover_type must be TPL or Comprehensive")
    live = await sb_get("policies", {"cover->>vehicle_id": f"eq.{vehicle['vehicle_id']}",
                                     "status": "in.(Active,Pending Payment)", "select": "policy_id,customer_id"},
                        owner=owner)
    if live:
        raise err(409, "policy_exists", f"This car already has an active Watheeq policy ({live[0]['policy_id']})",
                  policy_id=live[0]["policy_id"])
    tpl = money(vehicle.get("tpl_annual_sar"))
    comp = money(D(vehicle.get("market_value_sar")) * D(vehicle.get("comp_rate_percent")) / 100)
    total = comp if ct == "Comprehensive" else tpl
    start = today_local()
    end = _one_year_end(start)
    lbl = (f"{'Comprehensive' if ct == 'Comprehensive' else 'Third-party'} cover, 12 months — "
           f"{vehicle.get('year')} {vehicle.get('make_en')} {vehicle.get('model_en')}",
           f"{'تأمين شامل' if ct == 'Comprehensive' else 'تأمين ضد الغير'} لمدة 12 شهراً — "
           f"{vehicle.get('make_ar')} {vehicle.get('model_ar')} {vehicle.get('year')}")
    return {"total": total, "breakdown": [_line(lbl[0], lbl[1], total)],
            "summary": (f"{vehicle.get('year')} {vehicle.get('make_en')} {vehicle.get('model_en')}: third-party SAR "
                        f"{fmt_sar(tpl)}, full cover SAR {fmt_sar(comp)}. Quoted: {ct}.",
                        f"{vehicle.get('make_ar')} {vehicle.get('model_ar')} {vehicle.get('year')}: ضد الغير "
                        f"{fmt_sar(tpl)} ريال، الشامل {fmt_sar(comp)} ريال."),
            "params": {"cover_type": ct, "new_start": start.isoformat(), "new_end": end.isoformat(),
                       "plan_code": "MOTOR_COMP" if ct == "Comprehensive" else "MOTOR_TPL"},
            "extra": {"vehicle": vehicle_summary(vehicle), "cover_type": ct, "tpl_sar": tpl, "comp_sar": comp,
                      "owner_name_masked": mask_person_name(vehicle.get("owner_name_en")),
                      "transfer_status": vehicle.get("transfer_status")}}


async def _q_health_class_upgrade(customer, policy, vehicle, refs, params, owner) -> dict:
    if policy.get("product") != "Health":
        raise err(409, "wrong_product", "Class upgrades apply to health policies", product=policy.get("product"))
    _require_live(policy)
    to = str(params.get("to_class") or "").strip().upper().replace("CLASS", "").strip()
    to = {"أ": "A", "ب": "B", "ج": "C"}.get(to, to)
    cur = (policy.get("cover") or {}).get("class_code")
    fc, tc = refs["classes"].get(cur), refs["classes"].get(to)
    if not tc:
        raise err(400, "invalid_class", "to_class must be one of VIP, A, B, C", allowed=sorted(refs["classes"]))
    if int(tc.get("network_tier") or 0) <= int((fc or {}).get("network_tier") or 0):
        raise err(409, "not_an_upgrade", f"Class {to} is not higher than class {cur}", from_class=cur, to_class=to)
    members = await sb_get("health_members", {"policy_id": f"eq.{policy['policy_id']}", "status": "eq.Active"},
                           owner=owner)
    n = len(members)
    days = _days_left(policy)
    delta = D(tc.get("annual_premium_per_member_sar")) - D(fc.get("annual_premium_per_member_sar"))
    total = prorata(delta * n, days)
    ft, tt = int(fc.get("network_tier") or 0), int(tc.get("network_tier") or 0)
    gains = [{"provider_id": p["provider_id"], "name_en": p.get("name_en"), "name_ar": p.get("name_ar"),
              "city_en": p.get("city_en")}
             for p in refs["providers"].values() if ft < int(p.get("min_network_tier") or 0) <= tt]
    fam = n - 1
    return {"total": total,
            "breakdown": [
                _line(f"Class {to} − class {cur} annual premium per member", f"فرق القسط السنوي للعضو بين الفئة {to} والفئة {cur}", delta),
                _line(f"× {n} members", f"× {n} أعضاء", delta * n),
                _line(f"For the {days} days left (× {days}/365)", f"عن {days} يوماً المتبقية (× {days}/365)", total)],
            "summary": (f"Moving from class {cur} to class {to} for you and {fam} family members costs SAR {fmt_sar(total)} "
                        f"for the rest of the year. Network added: {', '.join(g['name_en'] for g in gains) or 'none'}.",
                        f"الترقية من الفئة {cur} إلى الفئة {to} لك و{fam} من أفراد العائلة: {fmt_sar(total)} ريال لبقية السنة. "
                        f"يُضاف إلى الشبكة: {'، '.join(g['name_ar'] for g in gains) or 'لا شيء'}."),
            "params": {"from_class": cur, "to_class": to, "members": [m["member_id"] for m in members], "days_left": days},
            "extra": {"from_class": cur, "to_class": to, "members_count": n, "family_members_count": fam,
                      "days_left": days, "network_gains": gains, "new_class": class_view(tc)}}


async def _q_add_member(customer, policy, vehicle, refs, params, owner) -> dict:
    if policy.get("product") != "Health":
        raise err(409, "wrong_product", "Members are added to health policies", product=policy.get("product"))
    _require_live(policy)
    rules = refs["rules"]
    name = (params.get("full_name_en") or params.get("name_en") or "").strip()
    dob = parse_date(params.get("dob"))
    cert = re.sub(r"\s", "", str(params.get("birth_cert_or_id") or params.get("national_id_or_birth_cert") or ""))
    rel_raw = (params.get("relationship") or "").strip()
    rel = MEMBER_RELATIONSHIP_SYNONYMS.get(rel_raw.lower(), rel_raw.capitalize() if rel_raw else "")
    if not name or not cert or not rel_raw:
        raise err(400, "missing_fields", "full_name_en, dob, birth_cert_or_id and relationship are required",
                  required=["full_name_en", "dob", "birth_cert_or_id", "relationship"])
    if rel not in ("Spouse", "Son", "Daughter", "Parent", "Sibling"):
        raise err(400, "invalid_relationship", "relationship must be Spouse, Son, Daughter, Parent or Sibling")
    today = today_local()
    if not dob or dob > today:
        raise err(400, "invalid_date", "dob must be a past date (YYYY-MM-DD)")
    deps = await sb_get("dependents", {"customer_id": f"eq.{customer['customer_id']}"}, owner=owner)
    for d in deps:
        if d.get("dob") == dob.isoformat() and (d.get("national_id_or_birth_cert") == cert or
                                                names_match(name, [d.get("full_name_en")])):
            ms = await sb_get("health_members", {"dependent_id": f"eq.{d['dependent_id']}",
                                                 "policy_id": f"eq.{policy['policy_id']}"}, owner=owner)
            if ms:
                raise err(409, "member_exists", f"{name} is already on the policy", member_id=ms[0]["member_id"])
    window = int(rules.get("newborn_add_window_days") or 30)
    age_days = (today - dob).days
    newborn = rel in ("Son", "Daughter") and age_days <= window
    cls = refs["classes"].get((policy.get("cover") or {}).get("class_code")) or {}
    days = _days_left(policy)
    total = prorata(cls.get("annual_premium_per_member_sar"), days)
    wp_until = None if newborn else (today + timedelta(days=MEMBER_WAITING_PERIOD_DAYS))
    extra = {"newborn": newborn, "waiting_period": not newborn, "age_days": age_days,
             "newborn_window_days": window,
             "days_left_in_newborn_window": max(0, window - age_days) if rel in ("Son", "Daughter") else None,
             "waiting_period_days": 0 if newborn else MEMBER_WAITING_PERIOD_DAYS,
             "waiting_period_until": wp_until.isoformat() if wp_until else None,
             "class_code": cls.get("class_code"), "class_name_en": cls.get("name_en"),
             "class_name_ar": cls.get("name_ar"), "days_left": days, "relationship": rel}
    if wp_until:
        put_labels(extra, "waiting_period_until", wp_until.isoformat())
    s_en = (f"{name} can be added on class {cls.get('class_code')}. "
            + ("Inside the newborn window, so cover starts today with no waiting period. "
               if newborn else f"A {MEMBER_WAITING_PERIOD_DAYS}-day waiting period applies. ")
            + f"SAR {fmt_sar(total)} for the rest of the policy year.")
    s_ar = (f"يمكن إضافة {name} على الفئة {cls.get('class_code')}. "
            + ("ضمن مهلة المولود، فتبدأ التغطية اليوم دون فترة انتظار. "
               if newborn else f"تنطبق فترة انتظار {MEMBER_WAITING_PERIOD_DAYS} يوماً. ")
            + f"{fmt_sar(total)} ريال لبقية مدة الوثيقة.")
    return {"total": total,
            "breakdown": [_line(f"Class {cls.get('class_code')} annual premium per member", f"القسط السنوي للعضو — الفئة {cls.get('class_code')}",
                                cls.get("annual_premium_per_member_sar")),
                          _line(f"For the {days} days left (× {days}/365)", f"عن {days} يوماً المتبقية (× {days}/365)", total)],
            "summary": (s_en, s_ar),
            "params": {"member": {"full_name_en": name, "full_name_ar": params.get("full_name_ar") or name,
                                  "dob": dob.isoformat(), "birth_cert_or_id": cert, "relationship": rel,
                                  "gender": params.get("gender")},
                       "newborn": newborn, "waiting_period_until": extra["waiting_period_until"]},
            "extra": extra}


async def _q_travel(customer, policy, vehicle, refs, params, owner) -> dict:
    rules = refs["rules"]
    pc = str(params.get("plan_code") or "TRAVEL_SCHENGEN").strip().upper()
    if "SCHENGEN" in pc:
        pc = "TRAVEL_SCHENGEN"
    elif "WORLD" in pc:
        pc = "TRAVEL_WORLDWIDE"
    plan = refs["plans"].get(pc)
    if not plan or plan.get("product") != "Travel":
        raise err(400, "invalid_plan", "plan_code must be a travel plan",
                  allowed=[k for k, v in refs["plans"].items() if v.get("product") == "Travel"])
    det = plan.get("details") or {}
    ts, te = parse_date(params.get("trip_start")), parse_date(params.get("trip_end"))
    today = today_local()
    if not ts or not te or te < ts or ts < today:
        raise err(400, "invalid_dates", "trip_start and trip_end are required, trip_start ≥ today and trip_end ≥ trip_start")
    travellers = params.get("travellers") or [{"name_en": customer["full_name_en"], "name_ar": customer["full_name_ar"],
                                               "relationship": "Policyholder"}]
    travellers = [{"name_en": t.get("name_en") or t.get("full_name_en"), "name_ar": t.get("name_ar"),
                   "relationship": t.get("relationship") or "Companion"} for t in travellers if isinstance(t, dict)]
    if not travellers or any(not t["name_en"] for t in travellers):
        raise err(400, "missing_fields", "each traveller needs name_en")
    schengen = pc == "TRAVEL_SCHENGEN"
    extra_days = int(rules.get("schengen_extra_days") or det.get("extra_days_after_trip") or 15) if schengen else 0
    p_start, p_end = ts, te + timedelta(days=extra_days)
    cover_days = (p_end - p_start).days + 1
    base = D(det.get("price_per_traveller_sar") or 155)
    covers = int(det.get("price_covers_days") or 31)
    per_day = D(det.get("extra_day_sar") or 5)
    per = base + max(0, cover_days - covers) * per_day
    n = len(travellers)
    total = money(per * n)
    lines = [_line(f"{plan.get('name_en')} — up to {covers} days, per traveller", f"{plan.get('name_ar')} — حتى {covers} يوماً، للمسافر", base)]
    if cover_days > covers:
        lines.append(_line(f"{cover_days - covers} extra days × SAR {fmt_sar(per_day)} per traveller",
                           f"{cover_days - covers} يوماً إضافية × {fmt_sar(per_day)} ريال للمسافر",
                           (cover_days - covers) * per_day))
    lines.append(_line(f"× {n} travellers", f"× {n} مسافرين", total))
    s, e = short_date_labels(p_start), short_date_labels(p_end)
    cov = _num(det.get("cover_eur"))
    extra = {"plan_code": pc, "is_schengen": schengen, "policy_start": p_start.isoformat(), "policy_end": p_end.isoformat(),
             "cover_days": cover_days, "extra_days_after_trip": extra_days, "cover_eur": cov,
             "schengen_min_cover_eur": rules.get("schengen_min_cover_eur") if schengen else None,
             "per_traveller_sar": money(per), "travellers": travellers}
    put_labels(extra, "policy_start", extra["policy_start"])
    put_labels(extra, "policy_end", extra["policy_end"])
    return {"total": total, "breakdown": lines,
            "summary": (f"Cover from {s[0]} to {e[0]}" + (f" (trip + {extra_days} days for the visa)" if schengen else "")
                        + f", €{cov:,} for {n} traveller{'s' if n > 1 else ''}: SAR {fmt_sar(total)}.",
                        f"تغطية من {s[1]} إلى {e[1]}" + (f" (الرحلة + {extra_days} يوماً للتأشيرة)" if schengen else "")
                        + f"، {cov:,} يورو لعدد {n} مسافر: {fmt_sar(total)} ريال."),
            "params": {"plan_code": pc, "policy_start": p_start.isoformat(), "policy_end": p_end.isoformat(),
                       "trip_start": ts.isoformat(), "trip_end": te.isoformat(), "travellers": travellers,
                       "destination_en": params.get("destination_en") or ("Schengen area" if schengen else "Worldwide"),
                       "destination_ar": params.get("destination_ar") or ("منطقة شنغن" if schengen else "حول العالم"),
                       "cover_eur": cov},
            "extra": extra}


_QUOTE_HANDLERS = {
    "renew": _q_renew, "upgrade_cover": _q_upgrade_cover, "add_driver": _q_add_driver,
    "add_addon": _q_add_addon, "cancel_refund": _q_cancel_refund, "new_policy_motor": _q_new_policy_motor,
    "health_class_upgrade": _q_health_class_upgrade, "add_member": _q_add_member, "travel": _q_travel,
}
_NEEDS_POLICY = {"renew", "upgrade_cover", "add_driver", "add_addon", "cancel_refund",
                 "health_class_upgrade", "add_member"}


# Top-level /quote fields; every other top-level key is a flat copy of a
# `params` field (platforms that strip nested objects send them flat).
_QUOTE_TOP_LEVEL = {"customer_id", "quote_type", "policy_id", "vehicle_id", "sequence_number",
                    "params", "caller_phone"}


async def _flat_extras(request: Request, top_level: set) -> dict:
    """Top-level body keys that are not declared fields (non-null values only)."""
    try:
        body = await request.json()
    except Exception:
        return {}
    if not isinstance(body, dict):
        return {}
    return {k: v for k, v in body.items() if k not in top_level and v is not None}


def _merge_flat(nested: Optional[dict], flat: dict) -> dict:
    """Nested object wins key-by-key; flat fields only fill what nested lacks."""
    out = {k: v for k, v in flat.items() if v is not None}
    out.update({k: v for k, v in (nested or {}).items() if v is not None})
    return out


@app.post("/quote")
async def create_quote(
    request: Request,
    customer_id: str = Body(...),
    quote_type: str = Body(...),
    policy_id: Optional[str] = Body(None),
    vehicle_id: Optional[str] = Body(None),
    sequence_number: Optional[str] = Body(None),
    params: Optional[dict] = Body(None),
    caller_phone: Optional[str] = Body(None),
):
    """Price a change. Persists a QTE- (24 h) with EN/AR breakdown lines; the
    agent quotes total_sar + summary and, on the customer's yes, calls
    /quote/apply with the quote_id. Type-specific inputs may be sent flat at
    the top level (primary shape) or nested in `params` (nested wins per key)."""
    owner = await resolve_owner(caller_phone)
    qt = (quote_type or "").strip().lower()
    if qt not in QUOTE_TYPES:
        raise err(400, "invalid_quote_type", f"quote_type must be one of {', '.join(QUOTE_TYPES)}",
                  allowed=list(QUOTE_TYPES))
    params = _merge_flat(params if isinstance(params, dict) else None,
                         await _flat_extras(request, _QUOTE_TOP_LEVEL))
    customer = await load_customer(customer_id, owner)
    refs = await _refs()
    policy = vehicle = None
    if qt in _NEEDS_POLICY:
        policy = await load_policy(policy_id, owner, customer["customer_id"])
        vid = (policy.get("cover") or {}).get("vehicle_id")
        if policy.get("product") == "Motor" and vid:
            vehicle = await sb_get_one("vehicles", {"vehicle_id": f"eq.{vid}"}, owner=owner)
    elif qt == "new_policy_motor":
        seq = re.sub(r"\D", "", str(params.get("sequence_number") or sequence_number or ""))
        if seq:
            vehicle = await sb_get_one("vehicles", {"sequence_number": f"eq.{seq}"}, owner=owner)
        elif vehicle_id:
            vehicle = await sb_get_one("vehicles", {"vehicle_id": f"eq.{vehicle_id}"}, owner=owner)
        else:
            raise err(400, "missing_fields", "sequence_number is required for new_policy_motor")
        if not vehicle:
            raise err(404, "vehicle_not_found", "No vehicle with that sequence number")

    q = await _QUOTE_HANDLERS[qt](customer, policy, vehicle, refs, params, owner)

    now = now_local()
    validity = int(refs["rules"].get("quote_validity_hours") or 24)
    expires = now + timedelta(hours=validity)
    quote_id = await next_id("quotes", "quote_id", "QTE-", owner)
    row = {
        "quote_id": quote_id,
        "customer_id": customer["customer_id"],
        "quote_type": qt,
        "policy_id": policy["policy_id"] if policy else None,
        "vehicle_id": vehicle["vehicle_id"] if vehicle else None,
        "params": {**q["params"], "input": params},
        "breakdown": q["breakdown"],
        "total_sar": q["total"],
        "status": "Open",
        "expires_at": iso(expires),
        "created_at": iso(now),
    }
    # Never persist a full national ID, even inside the raw input echo.
    if "national_id" in row["params"]["input"]:
        row["params"]["input"] = {**params, "national_id": mask_national_id(str(params["national_id"]))}
    await sb_insert("quotes", row, owner=owner)

    await log_agent_action(customer["customer_id"], "Quote Created",
                           f"Quote {quote_id} ({qt.replace('_', ' ')}) — SAR {fmt_sar(q['total'])}"
                           + (f" on {policy['policy_id']}" if policy else ""),
                           {"quote_id": quote_id, "quote_type": qt, "total_sar": q["total"],
                            "policy_id": row["policy_id"], "vehicle_id": row["vehicle_id"]},
                           owner=owner, reference_id=quote_id)
    exp_en, exp_ar = dt_labels(expires)
    return {
        "ok": True,
        "quote_id": quote_id,
        "quote_type": qt,
        "policy_id": row["policy_id"],
        "vehicle_id": row["vehicle_id"],
        "total_sar": q["total"],
        "is_refund": q["total"] < 0,
        "breakdown": q["breakdown"],
        "summary_en": q["summary"][0],
        "summary_ar": q["summary"][1],
        "expires_at": row["expires_at"],
        "expires_label_en": exp_en,
        "expires_label_ar": exp_ar,
        **q["extra"],
    }


# ============================================================
# WRITE: /quote/apply — execute exactly what was quoted
# ============================================================

def _sync_ok(policy_id: str) -> dict:
    return {"status": "Synced", "synced_at": iso(now_local()), "policy_id": policy_id}


async def _new_card_numbers(owner: str, n: int) -> list:
    if n <= 0:
        return []
    ids = await next_id("health_members", "card_number", "WTQ-HC-", owner, count=n)
    return ids if isinstance(ids, list) else [ids]


async def _apply_effects(q: dict, customer: dict, refs: dict, owner: str, pending: bool,
                         iban: Optional[str], holder: Optional[str]) -> dict:
    """Perform the writes for one quote. Returns {policy_id, effective_from,
    documents, action_type, description, extra, new_policy}."""
    qt = q["quote_type"]
    p = q.get("params") or {}
    total = D(q.get("total_sar"))
    today = today_local()
    now = now_local()
    cid = customer["customer_id"]
    policy = None
    if q.get("policy_id"):
        policy = await sb_get_one("policies", {"policy_id": f"eq.{q['policy_id']}"}, owner=owner)
        if not policy:
            raise err(404, "policy_not_found", f"Policy {q['policy_id']} no longer exists")
    vehicle = None
    vid = q.get("vehicle_id") or ((policy or {}).get("cover") or {}).get("vehicle_id")
    if vid:
        vehicle = await sb_get_one("vehicles", {"vehicle_id": f"eq.{vid}"}, owner=owner)
    new_status = "Pending Payment" if pending else "Active"

    if qt == "renew":
        new_id = await next_id("policies", "policy_id", policy_prefix(policy["product"]), owner)
        cover = dict(policy.get("cover") or {})
        cover.pop("renewal_offer", None)
        cover.pop("cancellation", None)
        if policy["product"] == "Motor":
            cover["cover_type"] = p.get("cover_type") or cover.get("cover_type")
            cover["claims_paid_count"] = 0
            if cover["cover_type"] == "TPL":
                cover["addons"] = []
        cover["renewed_from"] = policy["policy_id"]
        await sb_insert("policies", {
            "policy_id": new_id, "customer_id": cid, "product": policy["product"],
            "plan_code": p.get("plan_code") or policy.get("plan_code"), "status": new_status,
            "start_date": p["new_start"], "end_date": p["new_end"], "premium_paid_sar": money(total), "cover": cover,
        }, owner=owner)
        if policy["product"] == "Motor" and vehicle:
            await sb_update("vehicles", {"vehicle_id": f"eq.{vehicle['vehicle_id']}"}, {
                "insurance_status": "Insured",
                "insurance_sync": {"status": "Pending", "synced_at": None, "policy_id": new_id} if pending else _sync_ok(new_id),
                "registration_renewal_blocked": bool(pending and vehicle.get("registration_renewal_blocked")),
                "block_reason_en": vehicle.get("block_reason_en") if pending else None,
                "block_reason_ar": vehicle.get("block_reason_ar") if pending else None,
            }, owner=owner)
        return {"policy_id": new_id, "effective_from": parse_date(p["new_start"]), "new_policy": True,
                "documents": [{"type": "policy", "ref": new_id}], "action_type": "Policy Renewed",
                "description": f"Renewed {policy['policy_id']} as {new_id} ({p.get('cover_type') or policy['product']}) "
                               f"{p['new_start']} → {p['new_end']}, SAR {fmt_sar(total)}"
                               + ("; sent to the traffic system" if policy["product"] == "Motor" and not pending else ""),
                "extra": {"insurance_synced": policy["product"] == "Motor" and not pending,
                          "registration_unblocked": policy["product"] == "Motor" and not pending,
                          "renewed_from": policy["policy_id"]}}

    if qt == "upgrade_cover":
        cover = dict(policy.get("cover") or {})
        cover["cover_type"] = "Comprehensive"
        cover["repair"] = cover.get("repair") or "Approved Workshop"
        cover["upgraded_from"] = "TPL"
        cover["upgrade_effective_from"] = p.get("effective_from")
        await sb_update("policies", {"policy_id": f"eq.{policy['policy_id']}"}, {
            "plan_code": "MOTOR_COMP", "cover": cover,
            "premium_paid_sar": money(D(policy.get("premium_paid_sar")) + total)}, owner=owner)
        if vehicle:
            await sb_update("vehicles", {"vehicle_id": f"eq.{vehicle['vehicle_id']}"},
                            {"insurance_sync": _sync_ok(policy["policy_id"])}, owner=owner)
        eff = parse_ts(p.get("effective_from"))
        return {"policy_id": policy["policy_id"], "effective_from": eff, "new_policy": False,
                "documents": [{"type": "policy", "ref": policy["policy_id"]}], "action_type": "Cover Upgraded",
                "description": f"Upgraded {policy['policy_id']} from third-party to comprehensive from midnight tonight, "
                               f"SAR {fmt_sar(total)}",
                "extra": {"effective_from_label_en": f"Tonight at midnight ({date_labels(eff.date())[0]}, 12:00 AM)",
                          "effective_from_label_ar": f"الليلة عند منتصف الليل ({date_labels(eff.date())[1]}، 12:00 ص)"}}

    if qt == "add_driver":
        cover = dict(policy.get("cover") or {})
        drv = dict(p["driver"])
        drv["added_on"] = today.isoformat()
        cover["drivers"] = list(cover.get("drivers") or []) + [drv]
        await sb_update("policies", {"policy_id": f"eq.{policy['policy_id']}"}, {
            "cover": cover, "premium_paid_sar": money(D(policy.get("premium_paid_sar")) + total)}, owner=owner)
        return {"policy_id": policy["policy_id"], "effective_from": now, "new_policy": False,
                "documents": [{"type": "policy", "ref": policy["policy_id"]}], "action_type": "Driver Added",
                "description": f"Added driver {drv['name_en']} ({drv['relationship']}, ID ••{drv['id_last4']}) "
                               f"to {policy['policy_id']}, SAR {fmt_sar(total)}",
                "extra": {"drivers_count": len(cover["drivers"])}}

    if qt == "add_addon":
        cover = dict(policy.get("cover") or {})
        codes = [c for c in p.get("addons") or [] if c not in (cover.get("addons") or [])]
        cover["addons"] = list(cover.get("addons") or []) + codes
        if "AGENCY_REPAIR" in codes:
            cover["repair"] = "Agency"
        await sb_update("policies", {"policy_id": f"eq.{policy['policy_id']}"}, {
            "cover": cover, "premium_paid_sar": money(D(policy.get("premium_paid_sar")) + total)}, owner=owner)
        names = ", ".join(refs["addons"].get(c, {}).get("name_en", c) for c in codes)
        return {"policy_id": policy["policy_id"], "effective_from": now, "new_policy": False,
                "documents": [{"type": "policy", "ref": policy["policy_id"]}], "action_type": "Add-ons Added",
                "description": f"Added {names} to {policy['policy_id']}, SAR {fmt_sar(total)}",
                "extra": {"addons": cover["addons"], "repair": cover.get("repair")}}

    if qt == "new_policy_motor":
        new_id = await next_id("policies", "policy_id", "POL-MTR-", owner)
        cover = {"vehicle_id": vehicle["vehicle_id"], "cover_type": p["cover_type"],
                 "drivers": [{"name_en": customer["full_name_en"], "name_ar": customer["full_name_ar"],
                              "relationship": "Policyholder", "id_last4": customer.get("national_id_last4"),
                              "licence_issue_date": None}],
                 "addons": [], "repair": "Approved Workshop", "claims_paid_count": 0,
                 "issued_for_transfer": True}
        await sb_insert("policies", {
            "policy_id": new_id, "customer_id": cid, "product": "Motor", "plan_code": p["plan_code"],
            "status": new_status, "start_date": p["new_start"], "end_date": p["new_end"],
            "premium_paid_sar": money(total), "cover": cover}, owner=owner)
        await sb_update("vehicles", {"vehicle_id": f"eq.{vehicle['vehicle_id']}"}, {
            "owner_customer_id": cid,
            "insurance_status": "Insured",
            "insurance_sync": {"status": "Pending", "synced_at": None, "policy_id": new_id} if pending else _sync_ok(new_id),
        }, owner=owner)
        return {"policy_id": new_id, "effective_from": today, "new_policy": True,
                "documents": [{"type": "policy", "ref": new_id}], "action_type": "Policy Issued",
                "description": f"Issued {new_id} ({p['cover_type']}) for {vehicle.get('year')} {vehicle.get('make_en')} "
                               f"{vehicle.get('model_en')} (seq {vehicle.get('sequence_number')}) in the buyer's name, "
                               f"SAR {fmt_sar(total)}" + ("; sent to the traffic system" if not pending else ""),
                "extra": {"vehicle_id": vehicle["vehicle_id"], "transfer_ready": not pending,
                          "insurance_synced": not pending}}

    if qt == "health_class_upgrade":
        to = p["to_class"]
        tc = refs["classes"][to]
        cover = dict(policy.get("cover") or {})
        cover["class_code"] = to
        cover["network_tier"] = tc.get("network_tier")
        cover["upgraded_from_class"] = p.get("from_class")
        members = await sb_get("health_members", {"policy_id": f"eq.{policy['policy_id']}", "status": "eq.Active",
                                                  "order": "member_id.asc"}, owner=owner)
        cards = await _new_card_numbers(owner, len(members))
        await sb_update("policies", {"policy_id": f"eq.{policy['policy_id']}"}, {
            "cover": cover, "premium_paid_sar": money(D(policy.get("premium_paid_sar")) + total)}, owner=owner)
        await asyncio.gather(*[
            sb_update("health_members", {"member_id": f"eq.{m['member_id']}"},
                      {"class_code": to, "card_number": cards[i]}, owner=owner)
            for i, m in enumerate(members)])
        return {"policy_id": policy["policy_id"], "effective_from": today, "new_policy": False,
                "documents": [{"type": "policy", "ref": policy["policy_id"]}]
                + [{"type": "health_card", "ref": m["member_id"]} for m in members],
                "action_type": "Class Upgraded",
                "description": f"Upgraded {policy['policy_id']} from class {p.get('from_class')} to class {to} for "
                               f"{len(members)} members, new cards issued, SAR {fmt_sar(total)}",
                "extra": {"new_class": to, "members": [{"member_id": m["member_id"], "full_name_en": m.get("full_name_en"),
                                                        "card_number": cards[i]} for i, m in enumerate(members)],
                          "family_cards_document": {"type": "health_cards_family", "ref": policy["policy_id"]}}}

    if qt == "add_member":
        mem = p["member"]
        dep_id, mbr_id = await asyncio.gather(next_id("dependents", "dependent_id", "DEP-", owner),
                                              next_id("health_members", "member_id", "MBR-", owner))
        card = (await _new_card_numbers(owner, 1))[0]
        gender = mem.get("gender") or {"Son": "Male", "Daughter": "Female"}.get(mem["relationship"])
        if gender not in ("Male", "Female"):
            gender = None
        await sb_insert("dependents", {
            "dependent_id": dep_id, "customer_id": cid, "full_name_en": mem["full_name_en"],
            "full_name_ar": mem.get("full_name_ar") or mem["full_name_en"], "relationship": mem["relationship"],
            "dob": mem["dob"], "national_id_or_birth_cert": mem["birth_cert_or_id"], "gender": gender}, owner=owner)
        cls_code = (policy.get("cover") or {}).get("class_code")
        await sb_insert("health_members", {
            "member_id": mbr_id, "policy_id": policy["policy_id"], "customer_id": cid, "dependent_id": dep_id,
            "full_name_en": mem["full_name_en"], "full_name_ar": mem.get("full_name_ar") or mem["full_name_en"],
            "relationship": mem["relationship"], "dob": mem["dob"], "card_number": card, "class_code": cls_code,
            "limit_used_sar": 0, "dental_used_sar": 0, "optical_used_sar": 0, "cover_start": today.isoformat(),
            "waiting_period_until": p.get("waiting_period_until"), "status": "Active"}, owner=owner)
        cover = dict(policy.get("cover") or {})
        cover["members"] = list(cover.get("members") or []) + [mbr_id]
        await sb_update("policies", {"policy_id": f"eq.{policy['policy_id']}"}, {
            "cover": cover, "premium_paid_sar": money(D(policy.get("premium_paid_sar")) + total)}, owner=owner)
        return {"policy_id": policy["policy_id"], "effective_from": today, "new_policy": False,
                "documents": [{"type": "health_card", "ref": mbr_id}],
                "action_type": "Member Added",
                "description": f"Added {mem['full_name_en']} ({mem['relationship']}) to {policy['policy_id']} on class "
                               f"{cls_code}" + (" — newborn, no waiting period" if p.get("newborn") else
                                                f" — waiting period until {p.get('waiting_period_until')}")
                               + f", card {card}, SAR {fmt_sar(total)}",
                "extra": {"member_id": mbr_id, "dependent_id": dep_id, "card_number": card, "class_code": cls_code,
                          "waiting_period": not p.get("newborn"), "cover_starts": today.isoformat()}}

    if qt == "travel":
        new_id = await next_id("policies", "policy_id", "POL-TRV-", owner)
        cover = {"plan_code": p["plan_code"], "destination_en": p.get("destination_en"),
                 "destination_ar": p.get("destination_ar"), "travellers": p["travellers"],
                 "cover_eur": p.get("cover_eur"), "trip_start": p["trip_start"], "trip_end": p["trip_end"]}
        await sb_insert("policies", {
            "policy_id": new_id, "customer_id": cid, "product": "Travel", "plan_code": p["plan_code"],
            "status": new_status, "start_date": p["policy_start"], "end_date": p["policy_end"],
            "premium_paid_sar": money(total), "cover": cover}, owner=owner)
        docs = [{"type": "travel_certificate", "ref": new_id}] if p["plan_code"] == "TRAVEL_SCHENGEN" else []
        docs.append({"type": "policy", "ref": new_id})
        return {"policy_id": new_id, "effective_from": parse_date(p["policy_start"]), "new_policy": True,
                "documents": docs, "action_type": "Travel Policy Issued",
                "description": f"Issued {new_id} ({p['plan_code']}) {p['policy_start']} → {p['policy_end']} for "
                               f"{len(p['travellers'])} travellers, SAR {fmt_sar(total)}",
                "extra": {"policy_start": p["policy_start"], "policy_end": p["policy_end"]}}

    if qt == "cancel_refund":
        refund_amt = D(p.get("refund_sar") or 0)
        iban_n = normalize_iban(iban)
        if refund_amt > 0:
            if not iban_n:
                raise err(400, "iban_required", "The refund needs an IBAN in the policyholder's name")
            if not valid_saudi_iban(iban_n):
                raise err(400, "invalid_iban", "That IBAN is not valid — it must be SA followed by 22 digits")
            holder_name = holder or customer["full_name_en"]
            if not names_match(holder_name, [customer.get("full_name_en"), customer.get("full_name_ar")]):
                raise err(409, "iban_name_mismatch", "The IBAN must be in the policyholder's own name",
                          holder_name=holder_name, policyholder_name=customer.get("full_name_en"))
        cover = dict(policy.get("cover") or {})
        refund_id = None
        expected_by = add_working_days(today, 3)
        if refund_amt > 0:
            refund_id = await next_id("refunds", "refund_id", "RFD-", owner)
        cover["cancellation"] = {"reason": "Vehicle sold / ownership transferred", "cancelled_at": iso(now),
                                 "refund_id": refund_id, "refund_sar": money(refund_amt),
                                 "days_left_from_transfer": p.get("days_left_from_transfer")}
        await sb_update("policies", {"policy_id": f"eq.{policy['policy_id']}"},
                        {"status": "Cancelled", "cover": cover}, owner=owner)
        if refund_id:
            await sb_insert("refunds", {
                "refund_id": refund_id, "customer_id": cid, "policy_id": policy["policy_id"],
                "amount_sar": money(refund_amt), "iban_masked": mask_iban(iban_n), "status": "Initiated",
                "expected_by": expected_by.isoformat(), "created_at": iso(now)}, owner=owner)
            await sb_update("customers", {"customer_id": f"eq.{cid}"},
                            {"iban_masked": mask_iban(iban_n), "iban_holder_name": holder or customer["full_name_en"]},
                            owner=owner)
        if vehicle:
            await sb_update("vehicles", {"vehicle_id": f"eq.{vehicle['vehicle_id']}"}, {
                "insurance_status": "Not Insured",
                "insurance_sync": {"status": "Not Synced", "synced_at": iso(now), "policy_id": None}}, owner=owner)
        eb_en, eb_ar = date_labels(expected_by)
        return {"policy_id": policy["policy_id"], "effective_from": now, "new_policy": False,
                "documents": [{"type": "refund_notice", "ref": refund_id}] if refund_id else [],
                "action_type": "Policy Cancelled",
                "description": f"Cancelled {policy['policy_id']} after vehicle transfer; refund {refund_id or 'none'} "
                               f"SAR {fmt_sar(refund_amt)} to {mask_iban(iban_n) or 'n/a'}",
                "refund": {"refund_id": refund_id, "amount_sar": money(refund_amt),
                           "iban_masked": mask_iban(iban_n), "expected_by": expected_by.isoformat(),
                           "expected_by_label_en": eb_en, "expected_by_label_ar": eb_ar} if refund_id else None,
                "extra": {"policy_status": "Cancelled"}}

    raise err(400, "invalid_quote_type", f"Unsupported quote type {qt}")


@app.post("/quote/apply")
async def apply_quote(
    quote_id: str = Body(...),
    iban: Optional[str] = Body(None, description="cancel_refund only"),
    iban_holder_name: Optional[str] = Body(None, description="cancel_refund only"),
    caller_phone: Optional[str] = Body(None),
):
    """Execute a quote the customer agreed to. total > 0 -> Pending payment
    request with a portal pay_url; the policy stays Active unless
    business_rules.require_payment_before_issue = 1 (then new policies are
    'Pending Payment' until the pay page marks them paid)."""
    owner = await resolve_owner(caller_phone)
    q = await sb_get_one("quotes", {"quote_id": f"eq.{(quote_id or '').strip()}"}, owner=owner)
    if not q:
        raise err(404, "quote_not_found", f"No quote {quote_id}")
    if q.get("status") == "Applied":
        raise err(409, "quote_applied", f"Quote {quote_id} was already applied",
                  policy_id=(q.get("params") or {}).get("applied_policy_id"))
    now = now_local()
    exp = parse_ts(q.get("expires_at"))
    if q.get("status") == "Expired" or (exp and exp < now):
        if q.get("status") != "Expired":
            await sb_update("quotes", {"quote_id": f"eq.{q['quote_id']}"}, {"status": "Expired"}, owner=owner)
        raise err(409, "quote_expired", f"Quote {quote_id} expired — create a new one")

    customer = await load_customer(q["customer_id"], owner)
    refs = await _refs()
    require_pay = int(refs["rules"].get("require_payment_before_issue") or 0) == 1
    total = D(q.get("total_sar"))
    # Only brand-new policies can wait for payment; changes to an existing
    # policy take effect immediately with a payment link.
    pending = require_pay and total > 0 and q["quote_type"] in ("renew", "new_policy_motor", "travel")

    res = await _apply_effects(q, customer, refs, owner, pending, iban, iban_holder_name)

    payment = None
    if total > 0:
        pay_id = await next_id("payment_requests", "payment_id", "PAY-", owner)
        token = secrets.token_urlsafe(18)  # 24 chars
        desc_en = f"{res['action_type']} — {res['policy_id']} ({q['quote_id']})"
        desc_ar = f"{_ACTION_AR.get(res['action_type'], res['action_type'])} — {res['policy_id']} ({q['quote_id']})"
        await sb_insert("payment_requests", {
            "payment_id": pay_id, "customer_id": customer["customer_id"], "reference_type": "Quote",
            "reference_id": q["quote_id"], "amount_sar": money(total), "description_en": desc_en,
            "description_ar": desc_ar, "status": "Pending", "pay_token": token, "created_at": iso(now)}, owner=owner)
        payment = {"payment_id": pay_id, "amount_sar": money(total), "pay_url": pay_url(token), "status": "Pending"}

    qparams = dict(q.get("params") or {})
    qparams.update({"applied_policy_id": res["policy_id"], "applied_at": iso(now),
                    "payment_id": payment["payment_id"] if payment else None})
    await sb_update("quotes", {"quote_id": f"eq.{q['quote_id']}"}, {"status": "Applied", "params": qparams}, owner=owner)

    await log_agent_action(customer["customer_id"], res["action_type"], res["description"], {
        "quote_id": q["quote_id"], "quote_type": q["quote_type"], "policy_id": res["policy_id"],
        "total_sar": money(total), "payment_id": payment["payment_id"] if payment else None,
        "refund_id": (res.get("refund") or {}).get("refund_id"), "pending_payment": pending,
    }, owner=owner, reference_id=res["policy_id"])

    eff = res.get("effective_from")
    if isinstance(eff, datetime):
        ef_en, ef_ar = dt_labels(eff)
        eff_iso = iso(eff)
    else:
        ef_en, ef_ar = date_labels(eff)
        eff_iso = eff.isoformat() if eff else None
    extra = dict(res.get("extra") or {})
    return {
        "ok": True,
        "quote_id": q["quote_id"],
        "quote_type": q["quote_type"],
        "policy_id": res["policy_id"],
        "policy_status": "Pending Payment" if pending else extra.pop("policy_status", "Active"),
        "effective_from": eff_iso,
        "effective_from_label_en": extra.pop("effective_from_label_en", ef_en),
        "effective_from_label_ar": extra.pop("effective_from_label_ar", ef_ar),
        "total_sar": money(total),
        "payment": payment,
        "refund": res.get("refund"),
        "documents": res["documents"],
        **extra,
    }


_ACTION_AR = {
    "Policy Renewed": "تجديد الوثيقة", "Cover Upgraded": "الترقية إلى الشامل", "Driver Added": "إضافة سائق",
    "Add-ons Added": "إضافة منافع", "Policy Issued": "إصدار وثيقة", "Class Upgraded": "ترقية الفئة",
    "Member Added": "إضافة عضو", "Travel Policy Issued": "إصدار تأمين السفر", "Policy Cancelled": "إلغاء الوثيقة",
}


# ============================================================
# WRITE: /claim/open — five claim types (§3.5)
# ============================================================

CLAIM_TYPES = ("Motor", "Third Party", "Health Reimbursement", "Travel", "Home")
DOC_LABELS = {
    "driving_licence": ("Photo of driving licence", "صورة رخصة القيادة"),
    "iban": ("IBAN in the claimant's name", "رقم الآيبان باسم صاحب المطالبة"),
    "national_id": ("National ID", "الهوية الوطنية"),
    "damage_photos": ("Damage photos", "صور الأضرار"),
    "invoice": ("Invoice", "الفاتورة"),
    "medical_report": ("Medical report", "التقرير الطبي"),
    "baggage_report": ("Airline lost-baggage report (PIR)", "تقرير الأمتعة من شركة الطيران"),
    "boarding_pass": ("Boarding pass", "بطاقة صعود الطائرة"),
    "receipts": ("Receipts", "الإيصالات"),
    "najm_report": ("Najm accident report", "تقرير نجم"),
    "vehicle_registration": ("Vehicle registration", "استمارة المركبة"),
    "inspection_report": ("Inspection report", "تقرير المعاينة"),
    "market_evidence": ("Market evidence", "أدلة أسعار السوق"),
}
CLAIM_TYPE_AR = {"Motor": "مركبات", "Third Party": "طرف ثالث", "Health Reimbursement": "استرداد طبي",
                 "Travel": "سفر", "Home": "منزل"}


def _docs_from(stored: list, code: str) -> list:
    out = []
    for s in stored:
        out.append({"code": code, "path": s.get("path"), "url": s.get("url"), "stored": s.get("stored"),
                    "content_type": s.get("content_type"), "received_at": s.get("received_at")})
    return out


def _due_labels(d: date) -> dict:
    en, ar = date_labels(d)
    s_en, s_ar = short_date_labels(d)
    return {"payment_due_by": d.isoformat(), "payment_due_by_label_en": en, "payment_due_by_label_ar": ar,
            "payment_due_by_short_en": s_en, "payment_due_by_short_ar": s_ar}


async def _find_live_policy(owner: str, customer_id: str, product: str, policy_id: Optional[str]) -> dict:
    if policy_id:
        p = await load_policy(policy_id, owner, customer_id, product)
        if p.get("status") != "Active":
            raise err(409, "no_active_policy", f"{policy_id} is {p.get('status')}", policy_id=policy_id,
                      status=p.get("status"))
        return p
    rows = await sb_get("policies", {"customer_id": f"eq.{customer_id}", "product": f"eq.{product}",
                                     "status": "eq.Active", "order": "end_date.desc"}, owner=owner)
    if not rows:
        raise err(409, "no_active_policy", f"No active {product.lower()} policy for this customer", product=product)
    return rows[0]


@app.post("/claim/open")
async def open_claim(
    claim_type: str = Body(...),
    customer_id: Optional[str] = Body(None),
    policy_id: Optional[str] = Body(None),
    najm_report_number: Optional[str] = Body(None),
    member_id: Optional[str] = Body(None, description="Health Reimbursement: member treated (default principal)"),
    description: Optional[str] = Body(None),
    attachment_urls: Optional[list] = Body(None),
    amounts: Optional[dict] = Body(None),
    claimant: Optional[dict] = Body(None),
    settlement_preference: Optional[str] = Body(None),
    caller_phone: Optional[str] = Body(None),
    # flat alternatives to `amounts` / `claimant` (primary tool shape)
    invoice_total: Optional[Any] = Body(None),
    visit_date: Optional[str] = Body(None),
    provider_name: Optional[str] = Body(None),
    items: Optional[Any] = Body(None),
    incident: Optional[str] = Body(None),
    claimant_name_en: Optional[str] = Body(None),
    claimant_phone: Optional[str] = Body(None),
    claimant_national_id: Optional[str] = Body(None),
):
    """Open a claim. Motor + Third Party are filled from the Najm report; the
    others need attachments. Every amount returned is computed here.
    `amounts` / `claimant` fields may be sent flat (invoice_total, visit_date,
    provider_name, items, incident; claimant_name_en, claimant_phone,
    claimant_national_id) — nested wins per key."""
    owner = await resolve_owner(caller_phone)
    if isinstance(items, str):
        items = [i.strip() for i in items.split(",") if i.strip()]
    amounts = _merge_flat(amounts if isinstance(amounts, dict) else None,
                          {"invoice_total": invoice_total, "visit_date": visit_date,
                           "provider_name": provider_name, "items": items, "incident": incident}) or None
    claimant = _merge_flat(claimant if isinstance(claimant, dict) else None,
                           {"name_en": claimant_name_en, "phone": claimant_phone,
                            "national_id": claimant_national_id}) or None
    ct = next((t for t in CLAIM_TYPES if t.lower() == (claim_type or "").strip().lower()), None)
    if not ct:
        raise err(400, "invalid_claim_type", f"claim_type must be one of {', '.join(CLAIM_TYPES)}",
                  allowed=list(CLAIM_TYPES))
    refs = await _refs()
    rules = refs["rules"]
    today = today_local()
    now = now_local()
    pay_days = int(rules.get("claim_payment_days") or 15)
    sp = None
    if settlement_preference:
        sp = {"cash": "Cash", "repair": "Repair", "نقدي": "Cash", "كاش": "Cash", "إصلاح": "Repair"}.get(
            settlement_preference.strip().lower())
        if not sp:
            raise err(400, "invalid_settlement", "settlement_preference must be Cash or Repair")

    customer = None
    if ct != "Third Party":
        customer = await load_customer(customer_id, owner)

    row: dict = {"claim_type": ct, "customer_id": customer["customer_id"] if customer else None,
                 "opened_at": iso(now), "missing_documents": [], "documents": [], "customer_pays_sar": 0,
                 "settlement_preference": sp}
    out: dict = {}

    if ct in ("Motor", "Third Party"):
        if not najm_report_number:
            raise err(400, "najm_required", "The Najm report number is required")
        num = normalize_najm(najm_report_number)
        report = await sb_get_one("najm_reports", {"report_number": f"eq.{num}"}, owner=owner)
        if not report:
            raise err(404, "report_not_found", f"No Najm report {num}")
        nv = najm_view(report, rules)
        parties = nv["parties"]
        existing = await sb_get("claims", {"najm_report_number": f"eq.{num}", "claim_type": f"eq.{ct}",
                                           "select": "claim_id,status,customer_id"}, owner=owner)
        acc_en, acc_ar = dt_labels(parse_ts(report.get("accident_at")))
        out.update({"najm_report_number": num, "accident_at": report.get("accident_at"),
                    "accident_at_label_en": acc_en, "accident_at_label_ar": acc_ar,
                    "location_en": report.get("location_en"), "location_ar": report.get("location_ar"),
                    "damage_en": report.get("damage_en"), "damage_ar": report.get("damage_ar")})

        if ct == "Motor":
            vids = {v["vehicle_id"] for v in await sb_get(
                "vehicles", {"owner_customer_id": f"eq.{customer['customer_id']}", "select": "vehicle_id"}, owner=owner)}
            mine = next((p for p in parties if p.get("customer_id") == customer["customer_id"]
                         or (p.get("vehicle_id") and p.get("vehicle_id") in vids)), None)
            if not mine:
                raise err(409, "not_a_party", "This customer is not a party in that Najm report", report_number=num)
            dup = [c for c in existing if c.get("customer_id") == customer["customer_id"]]
            if dup:
                raise err(409, "claim_exists", f"Claim {dup[0]['claim_id']} is already open for this report",
                          claim_id=dup[0]["claim_id"], status=dup[0].get("status"))
            pols = await sb_get("policies", {"customer_id": f"eq.{customer['customer_id']}", "product": "eq.Motor",
                                             "status": "eq.Active", "cover->>vehicle_id": f"eq.{mine.get('vehicle_id')}"},
                                owner=owner)
            if policy_id:
                pols = [p for p in pols if p["policy_id"] == policy_id] or pols
            if not pols:
                raise err(409, "no_active_policy", "No active motor policy covers that vehicle",
                          vehicle_id=mine.get("vehicle_id"))
            pol = pols[0]
            fault = int(mine.get("fault_percent") or 0)
            pays = 0 if fault == 0 else money(rules.get("motor_excess_sar") or 500)
            other = next((p for p in parties if p is not mine), None)
            row.update({"policy_id": pol["policy_id"], "vehicle_id": mine.get("vehicle_id"),
                        "najm_report_number": num, "status": "Open", "fault_percent": fault,
                        "customer_pays_sar": pays,
                        "description_en": description or f"{report.get('damage_en')} — {report.get('location_en')}",
                        "description_ar": report.get("damage_ar"),
                        "documents": [{"code": "najm_report", "path": None, "stored": False, "source": "Najm",
                                       "received_at": iso(now)}],
                        "assigned_to_en": "Motor Claims — Riyadh",
                        "settlement_preference": sp or "Repair"})
            out.update({"fault_percent": fault, "other_party_fault_percent": (other or {}).get("fault_percent"),
                        "other_party_insurer_en": (other or {}).get("insurer_en"),
                        "next_step_en": "Book the damage inspection at an approved centre",
                        "next_step_ar": "حجز معاينة الأضرار في مركز معتمد", "next_booking_type": "Inspection"})
        else:  # Third Party
            at_fault = max(parties, key=lambda p: p["fault_percent"], default=None)
            if not at_fault or at_fault["fault_percent"] <= 0 or not at_fault.get("is_watheeq_customer"):
                raise err(409, "not_watheeq_at_fault", "The at-fault driver in that report is not insured with Watheeq",
                          report_number=num)
            victim = next((p for p in parties if p is not at_fault), None)
            if not victim:
                raise err(409, "not_a_party", "The report has no second party", report_number=num)
            if existing:
                raise err(409, "claim_exists", f"Claim {existing[0]['claim_id']} is already open for this report",
                          claim_id=existing[0]["claim_id"], status=existing[0].get("status"))
            pols = await sb_get("policies", {"customer_id": f"eq.{at_fault.get('customer_id')}", "product": "eq.Motor",
                                             "cover->>vehicle_id": f"eq.{at_fault.get('vehicle_id')}",
                                             "order": "end_date.desc"}, owner=owner)
            c_in = claimant or {}
            nid = re.sub(r"\D", "", str(c_in.get("national_id") or ""))
            if nid and not valid_national_id(nid):
                raise err(400, "invalid_national_id", "National ID / iqama must be 10 digits starting with 1 or 2")
            cl = {"name_en": c_in.get("name_en") or victim.get("name_en"), "name_ar": victim.get("name_ar"),
                  "phone": normalize_phone(c_in.get("phone")) or normalize_phone(caller_phone),
                  "national_id_last4": nid[-4:] if nid else None, "plate_en": victim.get("plate_en"),
                  "party": victim.get("party"), "insurer_en": victim.get("insurer_en")}
            missing = []
            if (sp or "") == "Cash":
                missing.append({"code": "iban", "label_en": DOC_LABELS["iban"][0], "label_ar": DOC_LABELS["iban"][1]})
            row.update({"policy_id": pols[0]["policy_id"] if pols else None, "vehicle_id": None,
                        "najm_report_number": num, "status": "Open", "fault_percent": int(victim["fault_percent"]),
                        "customer_pays_sar": 0, "claimant": cl, "missing_documents": missing,
                        "description_en": description or f"Third-party claim by {cl['name_en']} — {report.get('damage_en')}",
                        "description_ar": report.get("damage_ar"),
                        "documents": [{"code": "najm_report", "path": None, "stored": False, "source": "Najm",
                                       "received_at": iso(now)}],
                        "assigned_to_en": "Third-Party Claims — Riyadh"})
            out.update({"claimant_name_en": cl["name_en"], "at_fault_party": at_fault.get("party"),
                        "at_fault_percent": at_fault["fault_percent"],
                        "options_en": ["Cash (repair cost paid to your IBAN)", "Repair at a Watheeq approved workshop"],
                        "options_ar": ["نقداً (تكلفة الإصلاح تُحوَّل إلى الآيبان)", "الإصلاح في ورشة معتمدة لدى وثيق"],
                        "next_step_en": ("Send your IBAN, then book the damage inspection" if sp == "Cash"
                                         else "Book the damage inspection"),
                        "next_step_ar": ("أرسل رقم الآيبان ثم احجز معاينة الأضرار" if sp == "Cash"
                                         else "احجز معاينة الأضرار"),
                        "next_booking_type": "Inspection"})

    elif ct == "Health Reimbursement":
        pol = await _find_live_policy(owner, customer["customer_id"], "Health", policy_id)
        amt = amounts or {}
        try:
            invoice_total = D(amt.get("invoice_total"))
        except Exception:
            invoice_total = Decimal(0)
        if invoice_total <= 0:
            raise err(400, "invalid_amounts", "amounts.invoice_total must be a positive number")
        if not attachment_urls:
            raise err(400, "attachments_required", "Send photos of the invoice (and the medical report)")
        member = await _resolve_member(owner, member_id, customer["customer_id"])
        if not member or member.get("policy_id") != pol["policy_id"]:
            raise err(404, "member_not_found", "That person is not a member of the active health policy")
        cls = refs["classes"].get(member.get("class_code")) or {}
        pct = D(cls.get("out_of_network_reimbursement_percent") or 0)
        payable = money(invoice_total * pct / 100)
        mv = member_view(member, refs["classes"])
        payable = min(payable, mv["limit_remaining_sar"])
        due = today + timedelta(days=pay_days)
        vd = parse_date(amt.get("visit_date"))
        row.update({"policy_id": pol["policy_id"], "status": "Under Review", "documents_completed_at": iso(now),
                    "payment_due_by": due.isoformat(),
                    "description_en": description or f"Out-of-network reimbursement — {amt.get('provider_name') or 'clinic'}",
                    "description_ar": None,
                    "amounts": {"invoice_total": money(invoice_total), "eligible": money(invoice_total),
                                "percent": _num(pct), "payable": payable, "visit_date": vd.isoformat() if vd else None,
                                "provider_name": amt.get("provider_name"), "items": amt.get("items") or [],
                                "member_id": member["member_id"], "member_name_en": member.get("full_name_en")},
                    "approved_amount_sar": None,
                    "assigned_to_en": "Health Claims — Reimbursements"})
        vd_en, vd_ar = short_date_labels(vd) if vd else (None, None)
        out.update({"payable_sar": payable, "invoice_total_sar": money(invoice_total), "percent": _num(pct),
                    "visit_date": vd.isoformat() if vd else None, "visit_date_label_en": vd_en,
                    "visit_date_label_ar": vd_ar, "member_id": member["member_id"],
                    "formula_en": f"SAR {fmt_sar(invoice_total)} × {_num(pct)}% = SAR {fmt_sar(payable)}",
                    **_due_labels(due),
                    "next_step_en": f"Paid to the IBAN on file within {pay_days} days",
                    "next_step_ar": f"يُصرف إلى الآيبان المسجّل خلال {pay_days} يوماً"})

    elif ct == "Travel":
        pol = await _find_live_policy(owner, customer["customer_id"], "Travel", policy_id)
        if not attachment_urls:
            raise err(400, "attachments_required", "Send the airline's lost-bag report and your boarding pass")
        det = (refs["plans"].get(pol.get("plan_code")) or {}).get("details") or {}
        delayed = money(det.get("delayed_baggage_limit_sar") or rules.get("travel_delayed_bag_limit_sar") or 3000)
        lost = money(det.get("lost_baggage_limit_sar") or 5000)
        text = f"{description or ''} {(amounts or {}).get('incident') or ''}".lower()
        incident = "Lost baggage" if ("lost" in text or "ضاع" in text or "مفقود" in text) else (
            "Delayed baggage" if ("delay" in text or "تأخر" in text or "bag" in text) else "Other")
        due = today + timedelta(days=pay_days)
        row.update({"policy_id": pol["policy_id"], "status": "Under Review", "documents_completed_at": iso(now),
                    "payment_due_by": due.isoformat(),
                    "description_en": description or incident, "description_ar": None,
                    "amounts": {"incident": incident, "delayed_baggage_limit_sar": delayed,
                                "lost_baggage_limit_sar": lost, **({k: v for k, v in (amounts or {}).items()
                                                                     if k != "incident"})},
                    "assigned_to_en": "Travel Claims — Assistance"})
        out.update({"incident": incident, "limits": {"delayed_baggage_sar": delayed, "lost_baggage_sar": lost},
                    **_due_labels(due),
                    "next_step_en": "Keep receipts for essentials you buy while the bag is missing and send them here",
                    "next_step_ar": "احتفظ بإيصالات المشتريات الضرورية حتى وصول الحقيبة وأرسلها هنا"})

    else:  # Home
        pol = await _find_live_policy(owner, customer["customer_id"], "Home", policy_id)
        if not attachment_urls:
            raise err(400, "attachments_required", "Send photos of the damage (including the source, e.g. the pipe)")
        excess = money((pol.get("cover") or {}).get("excess_sar") or rules.get("home_excess_sar") or 500)
        row.update({"policy_id": pol["policy_id"], "status": "Open", "customer_pays_sar": excess,
                    "description_en": description or "Home damage", "description_ar": None,
                    "assigned_to_en": "Property Claims — Riyadh"})
        out.update({"excess_sar": excess,
                    "next_step_en": "Book the home inspector visit; keep damaged items until the inspection",
                    "next_step_ar": "احجز زيارة المعاين؛ احتفظ بالأغراض المتضررة حتى المعاينة",
                    "next_booking_type": "Home Inspection"})

    claim_id = await next_id("claims", "claim_id", claim_prefix(ct), owner)
    row["claim_id"] = claim_id
    stored = await store_attachments(attachment_urls, owner, claim_id)
    if stored:
        code = {"Health Reimbursement": "invoice", "Travel": "baggage_report", "Home": "damage_photos"}.get(ct, "damage_photos")
        row["documents"] = list(row.get("documents") or []) + _docs_from(stored, code)
    await sb_insert("claims", row, owner=owner)

    who = row["customer_id"] or (row.get("claimant") or {}).get("name_en")
    desc = f"Opened {ct} claim {claim_id}"
    if row.get("najm_report_number"):
        desc += f" from Najm {row['najm_report_number']}"
    if out.get("payable_sar") is not None:
        desc += f" — payable SAR {fmt_sar(out['payable_sar'])}"
    if stored:
        desc += f" with {len(stored)} attachment(s)"
    await log_agent_action(row["customer_id"], "Claim Opened", desc, {
        "claim_id": claim_id, "claim_type": ct, "policy_id": row.get("policy_id"),
        "najm_report_number": row.get("najm_report_number"), "claimant": who,
        "attachments": len(stored), "attachments_stored": sum(1 for s in stored if s.get("stored")),
    }, owner=owner, reference_id=claim_id)

    return {
        "ok": True,
        "claim_id": claim_id,
        "claim_type": ct,
        "status": row["status"],
        "policy_id": row.get("policy_id"),
        "customer_pays_sar": _num(row.get("customer_pays_sar")),
        "settlement_preference": row.get("settlement_preference"),
        "missing_documents": row.get("missing_documents"),
        "attachments_received": len(stored),
        "attachments_stored": sum(1 for s in stored if s.get("stored")),
        "documents": [{"type": "claim_summary", "ref": claim_id}],
        **({} if "payment_due_by" in out else {"payment_due_by": None, "payment_due_by_label_en": None,
                                                 "payment_due_by_label_ar": None}),
        **out,
    }


# ============================================================
# WRITE: /claim/update
# ============================================================

CLAIM_ACTIONS = ("add_document", "request_second_valuation", "accept_valuation", "object_rejection",
                 "set_iban", "set_settlement")


@app.post("/claim/update")
async def update_claim(
    claim_id: str = Body(...),
    action: str = Body(...),
    document_code: Optional[str] = Body(None),
    attachment_urls: Optional[list] = Body(None),
    reasons: Optional[str] = Body(None),
    iban: Optional[str] = Body(None),
    holder_name: Optional[str] = Body(None),
    preference: Optional[str] = Body(None),
    caller_phone: Optional[str] = Body(None),
):
    """add_document | request_second_valuation | accept_valuation |
    object_rejection | set_iban | set_settlement."""
    owner = await resolve_owner(caller_phone)
    act = (action or "").strip().lower()
    if act not in CLAIM_ACTIONS:
        raise err(400, "invalid_action", f"action must be one of {', '.join(CLAIM_ACTIONS)}", allowed=list(CLAIM_ACTIONS))
    claim = await sb_get_one("claims", {"claim_id": f"eq.{(claim_id or '').strip()}"}, owner=owner)
    if not claim:
        raise err(404, "claim_not_found", f"No claim {claim_id}")
    cid = claim["claim_id"]
    rules = await get_rules()
    today = today_local()
    now = now_local()
    pay_days = int(rules.get("claim_payment_days") or 15)
    if claim.get("status") in ("Paid", "Closed") and act != "object_rejection":
        raise err(409, "claim_closed", f"Claim {cid} is {claim.get('status')}", status=claim.get("status"))
    patch: dict = {}
    out: dict = {"ok": True, "claim_id": cid, "action": act}

    def _complete_docs(missing_after: list):
        """All documents in -> start the legal payment clock (calendar days)."""
        if missing_after or claim.get("documents_completed_at"):
            return None
        due = today + timedelta(days=pay_days)
        patch["documents_completed_at"] = iso(now)
        patch["payment_due_by"] = due.isoformat()
        if claim.get("status") in ("Awaiting Documents", "Open", "Under Review"):
            patch["status"] = "Approved" if claim.get("approved_amount_sar") else "Under Review"
        return due

    if act == "add_document":
        code = (document_code or "").strip().lower().replace(" ", "_")
        if not code:
            raise err(400, "missing_fields", "document_code is required")
        if not attachment_urls:
            raise err(400, "attachments_required", "Send the document as a photo or PDF")
        stored = await store_attachments(attachment_urls, owner, cid)
        missing = claim.get("missing_documents") or []
        was_missing = any(m.get("code") == code for m in missing)
        missing_after = [m for m in missing if m.get("code") != code]
        patch["missing_documents"] = missing_after
        patch["documents"] = list(claim.get("documents") or []) + _docs_from(stored, code)
        due = _complete_docs(missing_after)
        out.update({"document_code": code, "was_missing": was_missing, "remaining_missing": missing_after,
                    "documents_complete": not missing_after,
                    "attachments_stored": sum(1 for s in stored if s.get("stored"))})
        if due:
            out.update(_due_labels(due))
            out["approved_amount_sar"] = _num(claim.get("approved_amount_sar"))
        desc = f"Received {code.replace('_', ' ')} for {cid}" + (
            f"; documents complete — payment due by {due.isoformat()}" if due else "")
        action_type = "Document Added"

    elif act == "request_second_valuation":
        if claim.get("status") != "Total Loss — Valuation":
            raise err(409, "not_total_loss", f"Claim {cid} is not awaiting a total-loss valuation decision",
                      status=claim.get("status"))
        if not (reasons or "").strip():
            raise err(400, "missing_fields", "reasons is required")
        open_d = await sb_get("valuation_disputes", {"claim_id": f"eq.{cid}", "status": "in.(Open,With Expert)"},
                              owner=owner)
        if open_d:
            raise err(409, "dispute_exists", f"Second valuation {open_d[0]['dispute_id']} is already open",
                      dispute_id=open_d[0]["dispute_id"])
        stored = await store_attachments(attachment_urls, owner, cid)
        dsp = await next_id("valuation_disputes", "dispute_id", "DSP-", owner)
        due = add_working_days(today, int(rules.get("second_valuation_days") or 10))
        await sb_insert("valuation_disputes", {
            "dispute_id": dsp, "claim_id": cid, "customer_id": claim.get("customer_id"), "status": "Open",
            "reasons_en": reasons.strip(), "evidence": _docs_from(stored, "market_evidence"),
            "opened_at": iso(now), "due_by": due.isoformat()}, owner=owner)
        patch["status"] = "Second Valuation"
        en, ar = date_labels(due)
        out.update({"dispute_id": dsp, "due_by": due.isoformat(), "due_by_label_en": en, "due_by_label_ar": ar,
                    "working_days": int(rules.get("second_valuation_days") or 10),
                    "evidence_count": len(stored), "status": "Second Valuation",
                    "note_en": "An independent expert reviews it; nothing is paid until the decision.",
                    "note_ar": "يراجعه خبير مستقل؛ لا يُصرف أي مبلغ قبل القرار."})
        desc = f"Second valuation {dsp} opened on {cid} with {len(stored)} evidence file(s), due {due.isoformat()}"
        action_type = "Second Valuation Requested"

    elif act == "accept_valuation":
        if claim.get("status") != "Total Loss — Valuation":
            raise err(409, "not_total_loss", f"Claim {cid} has no valuation awaiting acceptance",
                      status=claim.get("status"))
        offered = D((claim.get("valuation") or {}).get("offered_amount"))
        due = today + timedelta(days=pay_days)
        patch.update({"status": "Approved", "approved_amount_sar": money(offered),
                      "documents_completed_at": claim.get("documents_completed_at") or iso(now),
                      "payment_due_by": due.isoformat()})
        out.update({"approved_amount_sar": money(offered), **_due_labels(due), "status": "Approved"})
        desc = f"Customer accepted the total-loss valuation on {cid} (SAR {fmt_sar(offered)}); payment due {due.isoformat()}"
        action_type = "Valuation Accepted"

    elif act == "object_rejection":
        if claim.get("status") != "Rejected":
            raise err(409, "not_rejected", f"Claim {cid} is not rejected", status=claim.get("status"))
        if not (reasons or "").strip():
            raise err(400, "missing_fields", "reasons is required")
        ex = await sb_get("complaints", {"related_claim_id": f"eq.{cid}", "complaint_type": "eq.Objection",
                                         "status": "neq.Resolved"}, owner=owner)
        if ex:
            raise err(409, "objection_exists", f"Objection {ex[0]['complaint_id']} is already open",
                      complaint_id=ex[0]["complaint_id"])
        obj = await next_id("complaints", "complaint_id", "OBJ-", owner)
        due = add_working_days(today, int(rules.get("complaint_sla_working_days") or 5))
        await sb_insert("complaints", {
            "complaint_id": obj, "customer_id": claim.get("customer_id"), "related_claim_id": cid,
            "complaint_type": "Objection", "description_en": reasons.strip(), "status": "Open",
            "opened_at": iso(now), "due_by": due.isoformat(),
            "assigned_to_en": "Watheeq Complaints (independent of Claims)",
            "assigned_to_ar": "إدارة الشكاوى في وثيق (مستقلة عن المطالبات)",
            "regulator_phone": rules.get("insurance_authority_phone")}, owner=owner)
        en, ar = date_labels(due)
        out.update({"objection_id": obj, "complaint_id": obj, "due_by": due.isoformat(), "due_by_label_en": en,
                    "due_by_label_ar": ar, "assigned_to_en": "Watheeq Complaints (independent of Claims)",
                    "assigned_to_ar": "إدارة الشكاوى في وثيق (مستقلة عن المطالبات)",
                    "regulator_phone": rules.get("insurance_authority_phone")})
        desc = f"Objection {obj} opened against the rejection of {cid}; due {due.isoformat()}"
        action_type = "Objection Opened"

    elif act == "set_iban":
        n = normalize_iban(iban)
        if not n:
            raise err(400, "iban_required", "iban is required")
        if not valid_saudi_iban(n):
            raise err(400, "invalid_iban", "That IBAN is not valid — it must be SA followed by 22 digits")
        cl = claim.get("claimant") or {}
        names = [cl.get("name_en"), cl.get("name_ar")]
        if claim.get("customer_id"):
            cu = await sb_get_one("customers", {"customer_id": f"eq.{claim['customer_id']}"}, owner=owner)
            names = [cu.get("full_name_en"), cu.get("full_name_ar")] if cu else names
        holder = holder_name or next((x for x in names if x), None)
        if not names_match(holder, names):
            raise err(409, "iban_name_mismatch", "The IBAN must be in the claimant's own name",
                      holder_name=holder, claimant_name=names[0])
        missing_after = [m for m in (claim.get("missing_documents") or []) if m.get("code") != "iban"]
        patch.update({"iban_masked": mask_iban(n), "missing_documents": missing_after})
        due = _complete_docs(missing_after) if claim.get("approved_amount_sar") else None
        out.update({"iban_masked": mask_iban(n), "remaining_missing": missing_after})
        if due:
            out.update(_due_labels(due))
        desc = f"IBAN {mask_iban(n)} saved on {cid}"
        action_type = "IBAN Saved"

    else:  # set_settlement
        sp = {"cash": "Cash", "repair": "Repair", "نقدي": "Cash", "كاش": "Cash", "إصلاح": "Repair"}.get(
            (preference or "").strip().lower())
        if not sp:
            raise err(400, "invalid_settlement", "preference must be Cash or Repair")
        missing = [m for m in (claim.get("missing_documents") or []) if m.get("code") != "iban"]
        if sp == "Cash" and not claim.get("iban_masked"):
            missing.append({"code": "iban", "label_en": DOC_LABELS["iban"][0], "label_ar": DOC_LABELS["iban"][1]})
        patch.update({"settlement_preference": sp, "missing_documents": missing})
        out.update({"settlement_preference": sp, "remaining_missing": missing})
        desc = f"Settlement preference on {cid} set to {sp}"
        action_type = "Settlement Preference Set"

    if patch:
        await sb_update("claims", {"claim_id": f"eq.{cid}"}, patch, owner=owner)
    out["status"] = patch.get("status", out.get("status", claim.get("status")))
    await log_agent_action(claim.get("customer_id"), action_type, desc, {"claim_id": cid, "action": act, **{
        k: v for k, v in out.items() if k in ("dispute_id", "objection_id", "payment_due_by", "document_code")}},
        owner=owner, reference_id=cid)
    return out


# ============================================================
# WRITE: /preauth/escalate (I-16)
# ============================================================

@app.post("/preauth/escalate")
async def escalate_preauth(
    preauth_id: str = Body(...),
    caller_phone: Optional[str] = Body(None),
):
    """Flag a pending pre-approval as 'patient waiting at hospital' (Urgent).
    Staff approve in the portal; the agent re-reads /customer to follow it."""
    owner = await resolve_owner(caller_phone)
    pa = await sb_get_one("preauths", {"preauth_id": f"eq.{(preauth_id or '').strip()}"}, owner=owner)
    if not pa:
        raise err(404, "preauth_not_found", f"No pre-approval {preauth_id}")
    refs = await _refs()
    escalated = False
    if pa.get("status") in ("Submitted", "Under Review") and not (pa.get("patient_waiting") and pa.get("priority") == "Urgent"):
        note = "Patient waiting at hospital (flagged via WhatsApp)"
        patch = {"patient_waiting": True, "priority": "Urgent",
                 "notes_en": ((pa.get("notes_en") or "") + (" | " if pa.get("notes_en") else "") + note)}
        await sb_update("preauths", {"preauth_id": f"eq.{pa['preauth_id']}"}, patch, owner=owner)
        pa = {**pa, **patch}
        escalated = True
        await log_agent_action(pa.get("customer_id"), "Pre-approval Escalated",
                               f"{pa['preauth_id']} ({pa.get('procedure_en')}) marked URGENT — patient waiting at hospital",
                               {"preauth_id": pa["preauth_id"], "provider_id": pa.get("provider_id")},
                               owner=owner, reference_id=pa["preauth_id"])
    member = await sb_get_one("health_members", {"member_id": f"eq.{pa.get('member_id')}"}, owner=owner)
    view = enrich_preauth(pa, refs, {member["member_id"]: member} if member else {})
    view.update({"ok": True, "escalated": escalated,
                 "already_decided": pa.get("status") in ("Approved", "Rejected")})
    return view


# ============================================================
# WRITE: /complaint (I-27)
# ============================================================

@app.post("/complaint")
async def open_complaint(
    customer_id: str = Body(...),
    description: str = Body(...),
    related_claim_id: Optional[str] = Body(None),
    caller_phone: Optional[str] = Body(None),
):
    """Formal complaint: due in complaint_sla_working_days (Sun–Thu), assigned
    to the claims manager, regulator number returned for escalation."""
    owner = await resolve_owner(caller_phone)
    customer = await load_customer(customer_id, owner)
    if not (description or "").strip():
        raise err(400, "missing_fields", "description is required")
    rules = await get_rules()
    today = today_local()
    now = now_local()
    claim = None
    if related_claim_id:
        claim = await sb_get_one("claims", {"claim_id": f"eq.{related_claim_id.strip()}"}, owner=owner)
        if not claim or claim.get("customer_id") != customer["customer_id"]:
            raise err(404, "claim_not_found", f"No claim {related_claim_id} for this customer")
        ex = await sb_get("complaints", {"related_claim_id": f"eq.{claim['claim_id']}",
                                         "complaint_type": "eq.Complaint", "status": "neq.Resolved"}, owner=owner)
        if ex:
            raise err(409, "complaint_exists", f"Complaint {ex[0]['complaint_id']} is already open for this claim",
                      complaint_id=ex[0]["complaint_id"], due_by=ex[0].get("due_by"))
    cmp_id = await next_id("complaints", "complaint_id", "CMP-", owner)
    sla = int(rules.get("complaint_sla_working_days") or 5)
    due = add_working_days(today, sla)
    await sb_insert("complaints", {
        "complaint_id": cmp_id, "customer_id": customer["customer_id"],
        "related_claim_id": claim["claim_id"] if claim else None, "complaint_type": "Complaint",
        "description_en": description.strip(), "status": "With Manager", "opened_at": iso(now),
        "due_by": due.isoformat(), "assigned_to_en": "Watheeq Claims Manager",
        "assigned_to_ar": "مدير المطالبات في وثيق",
        "regulator_phone": rules.get("insurance_authority_phone")}, owner=owner)
    ctx = None
    if claim:
        ec = enrich_claim(claim, {"rules": rules})
        ctx = {"claim_id": claim["claim_id"], "days_open": ec.get("days_open"),
               "limit_days": int(rules.get("claim_payment_days") or 15),
               "payment_due_by": ec.get("payment_due_by"), "overdue": ec.get("overdue"),
               "overdue_days": ec.get("overdue_days")}
    en, ar = date_labels(due)
    s_en, s_ar = short_date_labels(due)
    await log_agent_action(customer["customer_id"], "Complaint Opened",
                           f"Complaint {cmp_id}" + (f" on {claim['claim_id']}" if claim else "")
                           + f" assigned to the claims manager; answer due {due.isoformat()}",
                           {"complaint_id": cmp_id, "related_claim_id": claim["claim_id"] if claim else None,
                            "due_by": due.isoformat()}, owner=owner, reference_id=cmp_id)
    return {
        "ok": True, "complaint_id": cmp_id, "status": "With Manager",
        "due_by": due.isoformat(), "due_label_en": en, "due_label_ar": ar,
        "due_short_en": s_en, "due_short_ar": s_ar, "working_days": sla,
        "assigned_to_en": "Watheeq Claims Manager", "assigned_to_ar": "مدير المطالبات في وثيق",
        "regulator_phone": rules.get("insurance_authority_phone"),
        "overdue_context": ctx,
    }


# ============================================================
# PUBLIC: /public/pay/{token} — portal-hosted pay page (no PSP)
# ============================================================
# The pay_token IS the scope: no caller_phone, no tenant routing. The row's
# owner_id tells us which tenant's policy to activate.

PAY_METHODS = ("mada", "Apple Pay", "Credit Card", "STC Pay")


async def _payment_by_token(token: str) -> dict:
    if not token or not re.fullmatch(r"[A-Za-z0-9_\-]{16,64}", token):
        raise err(404, "payment_not_found", "Unknown payment link")
    rows = await _sb_raw_get("payment_requests", {"pay_token": f"eq.{token}", "limit": "1"})
    if not rows:
        raise err(404, "payment_not_found", "Unknown payment link")
    return rows[0]


@app.get("/public/pay/{token}")
async def public_pay_get(token: str):
    pr = await _payment_by_token(token)
    owner = pr["owner_id"]
    cust, quote = await asyncio.gather(
        sb_get_one("customers", {"customer_id": f"eq.{pr['customer_id']}",
                                 "select": "full_name_en,full_name_ar"}, owner=owner),
        sb_get_one("quotes", {"quote_id": f"eq.{pr['reference_id']}"}, owner=owner)
        if pr.get("reference_type") == "Quote" else asyncio.sleep(0, result=None),
    )
    out = {
        "payment_id": pr["payment_id"], "amount_sar": _num(pr.get("amount_sar")), "currency": "SAR",
        "description_en": pr.get("description_en"), "description_ar": pr.get("description_ar"),
        "status": pr.get("status"), "method": pr.get("method"),
        "customer_name_en": (cust or {}).get("full_name_en"), "customer_name_ar": (cust or {}).get("full_name_ar"),
        "reference_type": pr.get("reference_type"), "reference_id": pr.get("reference_id"),
        "line_items": (quote or {}).get("breakdown") or [],
        "policy_id": ((quote or {}).get("params") or {}).get("applied_policy_id"),
        "methods": list(PAY_METHODS),
        "brand": {"name_en": "Watheeq Insurance", "name_ar": "وثيق للتأمين", "primary": "#0E5A4A", "accent": "#C9A227"},
    }
    put_labels(out, "created", pr.get("created_at"), "datetime")
    if pr.get("paid_at"):
        put_labels(out, "paid_at", pr.get("paid_at"), "datetime")
    return out


@app.post("/public/pay/{token}")
async def public_pay_post(token: str, method: str = Body(..., embed=True)):
    """Mark paid, apply side effects (activate a Pending Payment policy and sync
    the vehicle to the traffic system), log 'Payment Received' from the Pay Page."""
    m = next((x for x in PAY_METHODS if x.lower() == (method or "").strip().lower()), None)
    if not m:
        raise err(400, "invalid_method", f"method must be one of {', '.join(PAY_METHODS)}", allowed=list(PAY_METHODS))
    pr = await _payment_by_token(token)
    owner = pr["owner_id"]
    if pr.get("status") == "Paid":
        raise err(409, "already_paid", "This payment is already complete", payment_id=pr["payment_id"])
    if pr.get("status") == "Cancelled":
        raise err(409, "payment_cancelled", "This payment request was cancelled", payment_id=pr["payment_id"])
    now = now_local()
    await sb_update("payment_requests", {"payment_id": f"eq.{pr['payment_id']}"},
                    {"status": "Paid", "method": m, "paid_at": iso(now)}, owner=owner)
    activated = None
    if pr.get("reference_type") == "Quote":
        q = await sb_get_one("quotes", {"quote_id": f"eq.{pr['reference_id']}"}, owner=owner)
        pid = ((q or {}).get("params") or {}).get("applied_policy_id")
    else:
        pid = pr.get("reference_id")
    if pid:
        pol = await sb_get_one("policies", {"policy_id": f"eq.{pid}"}, owner=owner)
        if pol and pol.get("status") == "Pending Payment":
            await sb_update("policies", {"policy_id": f"eq.{pid}"}, {"status": "Active"}, owner=owner)
            activated = pid
            vid = (pol.get("cover") or {}).get("vehicle_id")
            if pol.get("product") == "Motor" and vid:
                await sb_update("vehicles", {"vehicle_id": f"eq.{vid}"}, {
                    "insurance_status": "Insured", "insurance_sync": _sync_ok(pid),
                    "registration_renewal_blocked": False, "block_reason_en": None, "block_reason_ar": None},
                    owner=owner)
    await log_agent_action(pr.get("customer_id"), "Payment Received",
                           f"{pr['payment_id']} paid by {m} — SAR {fmt_sar(pr.get('amount_sar'))}"
                           + (f"; {activated} activated" if activated else ""),
                           {"payment_id": pr["payment_id"], "method": m, "amount_sar": _num(pr.get("amount_sar")),
                            "policy_id": pid, "activated_policy_id": activated},
                           owner=owner, reference_id=pr["payment_id"], source="Pay Page")
    en, ar = dt_labels(now)
    return {"ok": True, "payment_id": pr["payment_id"], "status": "Paid", "method": m,
            "amount_sar": _num(pr.get("amount_sar")), "paid_at": iso(now), "paid_at_label_en": en,
            "paid_at_label_ar": ar, "policy_id": pid, "activated_policy_id": activated,
            "receipt": {"type": "payment_receipt", "ref": pr["payment_id"]}}


# ============================================================
# Documents — branded PDFs (reportlab; Arabic via Amiri + reshaper + bidi)
# ============================================================

import arabic_reshaper                                  # noqa: E402
import qrcode                                           # noqa: E402
from bidi.algorithm import get_display                  # noqa: E402
from reportlab.lib.colors import HexColor, white        # noqa: E402
from reportlab.lib.pagesizes import A4                  # noqa: E402
from reportlab.lib.utils import ImageReader, simpleSplit  # noqa: E402
from reportlab.pdfbase import pdfmetrics                # noqa: E402
from reportlab.pdfbase.ttfonts import TTFont            # noqa: E402
from reportlab.pdfgen import canvas as rl_canvas        # noqa: E402

GREEN = HexColor("#0E5A4A")
GOLD = HexColor("#C9A227")
INK = HexColor("#1F2A2E")
MUTED = HexColor("#5B6B70")
LIGHT = HexColor("#EEF4F2")
RULE = HexColor("#D5E2DE")
FOOTER_EN = ("Watheeq Cooperative Insurance Co. (demo) · Licensed by the Insurance Authority (demo) · "
             "Demonstration document")
FOOTER_AR = "شركة وثيق للتأمين التعاوني (تجريبي) · مرخّصة من هيئة التأمين (تجريبي) · مستند تجريبي"

_FONTS = {"ok": False}
_AR_RE = re.compile(r"[؀-ۿݐ-ݿﭐ-﷿ﹰ-﻿]")


def _register_fonts():
    """Register the bundled Amiri TTFs (Arabic + Latin). Without them Arabic
    PDFs cannot be shaped; EN PDFs still render with Helvetica."""
    if _FONTS["ok"]:
        return
    try:
        pdfmetrics.registerFont(TTFont("Amiri", os.path.join(FONTS_DIR, "Amiri-Regular.ttf")))
        pdfmetrics.registerFont(TTFont("Amiri-Bold", os.path.join(FONTS_DIR, "Amiri-Bold.ttf")))
        _FONTS["ok"] = True
    except Exception as e:
        print(f"[pdf] Arabic font not available ({e}) — Arabic text will not render")


def has_ar(s: Optional[str]) -> bool:
    return bool(s) and bool(_AR_RE.search(str(s)))


def shape_ar(s: str) -> str:
    """Logical Arabic -> joined glyph forms -> visual (RTL) order for reportlab."""
    return get_display(arabic_reshaper.reshape(s)) if has_ar(s) else s


class PdfDoc:
    """Minimal branded page builder. RTL-aware: in Arabic documents labels sit
    on the right and every Arabic run is reshaped + bidi-reordered."""
    W, H = A4
    M = 42

    def __init__(self, lang: str, title_en: str, title_ar: str, ref: str):
        _register_fonts()
        self.rtl = lang == "ar" and _FONTS["ok"]
        self.buf = io.BytesIO()
        self.c = rl_canvas.Canvas(self.buf, pagesize=A4)
        self.c.setTitle(f"{title_en} — {ref}")
        self.c.setAuthor("Watheeq Insurance (demo)")
        self.title_en, self.title_ar, self.ref = title_en, title_ar, ref
        self.page_no = 0
        self.y = 0
        self._start_page()

    # ---- text primitives -------------------------------------------------
    def T(self, en, ar=None) -> str:
        v = ar if (self.rtl and ar) else en
        return "" if v is None else str(v)

    def font(self, s: str, bold: bool = False) -> str:
        if _FONTS["ok"] and (self.rtl or has_ar(s)):
            return "Amiri-Bold" if bold else "Amiri"
        return "Helvetica-Bold" if bold else "Helvetica"

    def draw(self, x, y, s, size=10.0, bold=False, color=INK, align=None):
        s = "" if s is None else str(s)
        f = self.font(s, bold)
        self.c.setFont(f, size)
        self.c.setFillColor(color)
        disp = shape_ar(s) if f.startswith("Amiri") else s
        align = align or ("right" if self.rtl else "left")
        if align == "right":
            self.c.drawRightString(x, y, disp)
        elif align == "center":
            self.c.drawCentredString(x, y, disp)
        else:
            self.c.drawString(x, y, disp)

    def wrap(self, s: str, size: float, width: float, bold=False) -> list:
        s = "" if s is None else str(s)
        return simpleSplit(s, self.font(s, bold), size, width) or [""]

    @property
    def x0(self):
        return self.W - self.M if self.rtl else self.M

    @property
    def x1(self):
        return self.M if self.rtl else self.W - self.M

    def sar(self, v) -> str:
        if v is None:
            return "—"
        return f"{fmt_sar(v)} ريال" if self.rtl else f"SAR {fmt_sar(v)}"

    def date(self, v) -> str:
        d = parse_date(v)
        if not d:
            return "—"
        en, ar = date_labels(d, with_year=True)
        return self.T(en, ar)

    def dtm(self, v) -> str:
        t = parse_ts(v)
        if not t:
            return "—"
        en, ar = date_labels(t.date(), with_year=True)
        ten, tar = time_labels(t)
        return self.T(f"{en}, {ten}", f"{ar}، {tar}")

    # ---- page furniture ----------------------------------------------------
    def _start_page(self):
        self.page_no += 1
        c, W, H, M = self.c, self.W, self.H, self.M
        c.setFillColor(GREEN)
        c.rect(0, H - 72, W, 72, fill=1, stroke=0)
        c.setFillColor(GOLD)
        c.rect(0, H - 76, W, 4, fill=1, stroke=0)
        self.draw(M, H - 40, "Watheeq Insurance", 19, True, white, "left")
        self.draw(M, H - 58, "Motor · Health · Travel · Home", 8.5, False, HexColor("#CFE3DD"), "left")
        if _FONTS["ok"]:
            self.draw(W - M, H - 44, "وثيق للتأمين", 22, True, white, "right")
        self.y = H - 110
        self.draw(self.x0, self.y, self.T(self.title_en, self.title_ar), 16, True, GREEN)
        self.draw(self.x1, self.y, self.ref, 10, False, MUTED, "left" if self.rtl else "right")
        self.y -= 10
        c.setStrokeColor(GOLD)
        c.setLineWidth(1.2)
        c.line(M, self.y, W - M, self.y)
        self.y -= 20
        # footer
        c.setStrokeColor(RULE)
        c.setLineWidth(0.6)
        c.line(M, 46, W - M, 46)
        self.draw(W / 2, 34, FOOTER_EN, 7.2, False, MUTED, "center")
        if _FONTS["ok"]:
            self.draw(W / 2, 22, FOOTER_AR, 7.5, False, MUTED, "center")
        gen = now_local()
        self.draw(M, 10, f"Generated {gen.strftime('%Y-%m-%d %H:%M')} (Riyadh)", 6.5, False, MUTED, "left")
        self.draw(W - M, 10, f"Page {self.page_no}", 6.5, False, MUTED, "right")

    def new_page(self):
        self.c.showPage()
        self._start_page()

    def ensure(self, h: float):
        if self.y - h < 60:
            self.new_page()

    # ---- blocks -------------------------------------------------------------
    def section(self, en, ar=None):
        self.ensure(40)
        self.y -= 6
        self.draw(self.x0, self.y, self.T(en, ar), 12, True, GREEN)
        self.y -= 6
        self.c.setStrokeColor(RULE)
        self.c.setLineWidth(0.8)
        self.c.line(self.M, self.y, self.W - self.M, self.y)
        self.y -= 15

    def kv(self, label_en, label_ar, value, bold=False):
        label = self.T(label_en, label_ar)
        v = "—" if value in (None, "") else str(value)
        lw = 170
        width = self.W - 2 * self.M - lw
        lines = self.wrap(v, 10, width, bold)
        self.ensure(15 * len(lines) + 2)
        self.draw(self.x0, self.y, label, 9.5, False, MUTED)
        vx = (self.W - self.M - lw) if self.rtl else (self.M + lw)
        for i, ln in enumerate(lines):
            self.draw(vx, self.y, ln, 10, bold, INK)
            if i < len(lines) - 1:
                self.y -= 13
        self.y -= 16

    def para(self, en, ar=None, size=9.5, color=INK, bold=False):
        text = self.T(en, ar)
        for ln in self.wrap(text, size, self.W - 2 * self.M, bold):
            self.ensure(14)
            self.draw(self.x0, self.y, ln, size, bold, color)
            self.y -= size + 4
        self.y -= 3

    def callout(self, en, ar=None, color=GOLD):
        text = self.T(en, ar)
        lines = self.wrap(text, 9.5, self.W - 2 * self.M - 24)
        h = 14 * len(lines) + 12
        self.ensure(h + 6)
        top = self.y + 10
        self.c.setFillColor(LIGHT)
        self.c.setStrokeColor(color)
        self.c.setLineWidth(1)
        self.c.roundRect(self.M, top - h, self.W - 2 * self.M, h, 6, fill=1, stroke=1)
        self.y -= 4
        x = self.W - self.M - 12 if self.rtl else self.M + 12
        for ln in lines:
            self.draw(x, self.y, ln, 9.5, False, INK)
            self.y -= 14
        self.y -= 12

    def table(self, cols: list, rows: list, size: float = 9):
        """cols: [(label_en, label_ar, width_fraction)], rows: [[cell,...]]."""
        total_w = self.W - 2 * self.M
        widths = [total_w * c[2] for c in cols]
        xs = []
        acc = 0.0
        for w in widths:
            xs.append((self.W - self.M - acc - 6) if self.rtl else (self.M + acc + 6))
            acc += w
        self.ensure(40)
        self.c.setFillColor(GREEN)
        self.c.rect(self.M, self.y - 5, total_w, 18, fill=1, stroke=0)
        for i, col in enumerate(cols):
            self.draw(xs[i], self.y + 0.5, self.T(col[0], col[1]), size, True, white)
        self.y -= 20
        for r_i, row in enumerate(rows):
            cells = [self.wrap("—" if c in (None, "") else str(c), size, widths[i] - 10) for i, c in enumerate(row)]
            h = max(len(c) for c in cells) * (size + 3) + 5
            self.ensure(h + 4)
            if r_i % 2 == 0:
                self.c.setFillColor(LIGHT)
                self.c.rect(self.M, self.y - h + size + 1, total_w, h, fill=1, stroke=0)
            for i, lines in enumerate(cells):
                yy = self.y
                for ln in lines:
                    self.draw(xs[i], yy, ln, size, False, INK)
                    yy -= size + 3
            self.y -= h
        self.y -= 10

    def qr(self, data: str, x: float, y: float, size: float):
        img = qrcode.make(data, box_size=6, border=1)
        pil = getattr(img, "_img", None) or (img.get_image() if hasattr(img, "get_image") else img)
        bio = io.BytesIO()
        pil.save(bio, format="PNG")
        bio.seek(0)
        self.c.drawImage(ImageReader(bio), x, y, size, size)

    def finish(self) -> bytes:
        self.c.showPage()
        self.c.save()
        return self.buf.getvalue()


DOC_TYPES = {
    "policy": ("Policy Schedule", "جدول الوثيقة"),
    "health_card": ("Health Insurance Card", "بطاقة التأمين الصحي"),
    "health_cards_family": ("Family Health Cards", "بطاقات التأمين الصحي للعائلة"),
    "travel_certificate": ("Certificate of Travel Insurance", "شهادة تأمين السفر"),
    "limits_sheet": ("Health Cover — Limits Used and Remaining", "حدود التغطية الصحية — المستخدم والمتبقي"),
    "booking_confirmation": ("Booking Confirmation", "تأكيد الحجز"),
    "rental_voucher": ("Replacement Car Voucher", "قسيمة السيارة البديلة"),
    "claim_summary": ("Claim Summary", "ملخص المطالبة"),
    "payment_receipt": ("Payment Receipt", "إيصال دفع"),
    "refund_notice": ("Refund Notice", "إشعار استرداد"),
    "accident_pack": ("Accident Pack — ID, Registration, Report", "ملف الحادث — الهوية والاستمارة والتقرير"),
}

_POLICY_PREFIX_PRODUCT = {"POL-MTR-": "Motor", "POL-HLT-": "Health", "POL-TRV-": "Travel", "POL-HOM-": "Home"}


def _doc_label(doc_type: str, ref: str) -> str:
    """Staff-facing name of a generated document, e.g. 'Motor policy' / 'Claim Summary'."""
    if doc_type == "policy":
        product = next((v for k, v in _POLICY_PREFIX_PRODUCT.items() if ref.upper().startswith(k)), None)
        return f"{product} policy" if product else "Policy"
    return DOC_TYPES[doc_type][0]


PRODUCT_AR = {"Motor": "المركبات", "Health": "الصحي", "Travel": "السفر", "Home": "المنزل"}
STATUS_AR = {"Active": "سارية", "Expired": "منتهية", "Cancelled": "ملغاة", "Pending Payment": "بانتظار الدفع",
             "Open": "مفتوحة", "Awaiting Documents": "بانتظار المستندات", "Inspection Booked": "تم حجز المعاينة",
             "Under Review": "قيد المراجعة", "Approved": "معتمدة", "Paid": "مدفوعة", "Rejected": "مرفوضة",
             "Total Loss — Valuation": "خسارة كلية — التقييم", "Second Valuation": "إعادة التقييم",
             "Closed": "مغلقة", "Booked": "محجوز", "Completed": "مكتمل", "Initiated": "قيد التحويل",
             "Sent": "تم التحويل", "Pending": "بانتظار الدفع"}
REL_AR = {"Principal": "المؤمَّن الرئيسي", "Spouse": "الزوج/الزوجة", "Son": "ابن", "Daughter": "ابنة",
          "Parent": "أحد الوالدين", "Sibling": "أخ/أخت", "Child": "ابن/ابنة", "Policyholder": "حامل الوثيقة",
          "Employee (work contract)": "موظف بعقد عمل", "Companion": "مرافق"}


def _st(doc: PdfDoc, v: Optional[str]) -> str:
    return doc.T(v, STATUS_AR.get(v or "")) if v else "—"


def _rel(doc: PdfDoc, v: Optional[str]) -> str:
    return doc.T(v, REL_AR.get(v or "")) if v else "—"


def _name(doc: PdfDoc, row: dict, en_key="full_name_en", ar_key="full_name_ar") -> str:
    return doc.T(row.get(en_key), row.get(ar_key))


async def _load(table: str, key: str, ref: str, owner: str, code: str, label: str) -> dict:
    row = await sb_get_one(table, {key: f"eq.{ref}"}, owner=owner)
    if not row:
        raise err(404, code, f"No {label} {ref}")
    return row


def _health_card_page(doc: PdfDoc, m: dict, pol: dict, cls: dict, customer: dict):
    """One card, drawn credit-card style (scaled), with a verification QR."""
    c = doc.c
    cw, ch = 400, 250
    x = (doc.W - cw) / 2
    top = doc.y + 4
    yb = top - ch
    c.setFillColor(GREEN)
    c.roundRect(x, yb, cw, ch, 14, fill=1, stroke=0)
    c.setFillColor(GOLD)
    c.rect(x, yb + ch - 50, cw, 3, fill=1, stroke=0)
    doc.draw(x + 18, yb + ch - 30, "Watheeq Insurance", 15, True, white, "left")
    if _FONTS["ok"]:
        doc.draw(x + cw - 18, yb + ch - 32, "وثيق للتأمين", 17, True, white, "right")
    doc.draw(x + 18, yb + ch - 70, "HEALTH INSURANCE CARD", 9, True, GOLD, "left")
    if _FONTS["ok"]:
        doc.draw(x + cw - 18, yb + ch - 70, "بطاقة التأمين الصحي", 10, True, GOLD, "right")
    doc.draw(x + 18, yb + ch - 96, m.get("full_name_en") or "", 15, True, white, "left")
    if _FONTS["ok"] and m.get("full_name_ar"):
        doc.draw(x + 18, yb + ch - 116, m.get("full_name_ar"), 13, False, white, "left")
    rows = [("Card no.", m.get("card_number")), ("Class", f"{cls.get('class_code')} — tier {cls.get('network_tier')}"),
            ("Policy", pol.get("policy_id")), ("Valid to", pol.get("end_date")),
            ("Member", f"{m.get('member_id')} · {m.get('relationship')}"),
            ("Co-pay", f"SAR {fmt_sar(cls.get('copay_per_visit_sar') or 0)} per visit")]
    yy = yb + ch - 142
    for k, v in rows:
        doc.draw(x + 18, yy, k, 7.5, False, HexColor("#CFE3DD"), "left")
        doc.draw(x + 88, yy, str(v or "—"), 9, True, white, "left")
        yy -= 15
    qsize = 96
    c.setFillColor(white)
    c.roundRect(x + cw - qsize - 22, yb + 20, qsize + 8, qsize + 8, 6, fill=1, stroke=0)
    doc.qr(f"WATHEEQ|{m.get('card_number')}|{m.get('member_id')}|{pol.get('policy_id')}|{cls.get('class_code')}|"
           f"{pol.get('end_date')}", x + cw - qsize - 18, yb + 24, qsize)
    doc.y = yb - 24
    doc.kv("Policyholder", "حامل الوثيقة", _name(doc, customer))
    doc.kv("Network", "الشبكة", doc.T(f"Class {cls.get('class_code')} network (tier {cls.get('network_tier')})",
                                        f"شبكة الفئة {cls.get('class_code')} (المستوى {cls.get('network_tier')})"))
    wp = parse_date(m.get("waiting_period_until"))
    doc.kv("Cover starts", "بداية التغطية", doc.date(m.get("cover_start")))
    doc.kv("Waiting period", "فترة الانتظار",
           doc.T(f"Until {doc.date(wp)}", f"حتى {doc.date(wp)}") if wp and wp > today_local()
           else doc.T("None", "لا يوجد"))
    doc.callout("Digital card — hospitals and clinics in the network accept it from the phone screen. "
                "Show it with your ID at reception.",
                "بطاقة رقمية — تقبلها المستشفيات والعيادات في الشبكة من شاشة الجوال. اعرضها مع الهوية عند الاستقبال.")


async def _render(doc_type: str, ref: str, lang: str, owner: str) -> tuple:
    """Build the PDF. Returns (bytes, customer_id)."""
    refs = await _refs()
    title_en, title_ar = DOC_TYPES[doc_type]
    rules = refs["rules"]

    if doc_type in ("policy", "travel_certificate", "limits_sheet", "health_cards_family"):
        pol = await _load("policies", "policy_id", ref, owner, "policy_not_found", "policy")
        customer = await _load("customers", "customer_id", pol["customer_id"], owner, "customer_not_found", "customer")
        plan = refs["plans"].get(pol.get("plan_code")) or {}
        cover = pol.get("cover") or {}
        if doc_type == "travel_certificate" and pol.get("product") != "Travel":
            raise err(409, "wrong_product", f"{ref} is not a travel policy", product=pol.get("product"))
        if doc_type in ("limits_sheet", "health_cards_family") and pol.get("product") != "Health":
            raise err(409, "wrong_product", f"{ref} is not a health policy", product=pol.get("product"))
        if doc_type == "travel_certificate":
            title_en, title_ar = (("Certificate of Travel Insurance — Schengen Visa", "شهادة تأمين السفر — تأشيرة شنغن")
                                  if pol.get("plan_code") == "TRAVEL_SCHENGEN" else DOC_TYPES[doc_type])
        doc = PdfDoc(lang, title_en, title_ar, ref)

        if doc_type == "health_cards_family":
            members = await sb_get("health_members", {"policy_id": f"eq.{ref}", "status": "eq.Active",
                                                      "order": "member_id.asc"}, owner=owner)
            cls = refs["classes"].get(cover.get("class_code")) or {}
            for i, m in enumerate(members):
                if i:
                    doc.new_page()
                _health_card_page(doc, m, pol, refs["classes"].get(m.get("class_code")) or cls, customer)
            return doc.finish(), customer["customer_id"]

        if doc_type == "limits_sheet":
            members = await sb_get("health_members", {"policy_id": f"eq.{ref}", "status": "eq.Active",
                                                      "order": "member_id.asc"}, owner=owner)
            cls = refs["classes"].get(cover.get("class_code")) or {}
            doc.kv("Policyholder", "حامل الوثيقة", _name(doc, customer))
            doc.kv("Class", "الفئة", f"{cls.get('class_code')} — {doc.T(cls.get('name_en'), cls.get('name_ar'))}")
            doc.kv("Policy period", "مدة الوثيقة", f"{doc.date(pol.get('start_date'))} – {doc.date(pol.get('end_date'))}")
            doc.kv("As of", "بتاريخ", doc.date(today_local()))
            doc.para("Each member has their own limits — they are not shared.",
                     "لكل عضو حدوده الخاصة — ولا تُشارك بين الأعضاء.", color=MUTED)
            doc.section("Annual limit", "الحد السنوي")
            rows = []
            views = [member_view(m, refs["classes"]) for m in members]
            for v in views:
                rows.append([doc.T(v["full_name_en"], v["full_name_ar"]), _rel(doc, v["relationship"]),
                             doc.sar(v["annual_limit_sar"]), doc.sar(v["limit_used_sar"]), doc.sar(v["limit_remaining_sar"])])
            doc.table([("Member", "العضو", 0.30), ("Relationship", "الصلة", 0.16), ("Limit", "الحد", 0.18),
                       ("Used", "المستخدم", 0.18), ("Remaining", "المتبقي", 0.18)], rows)
            doc.section("Dental and optical", "الأسنان والنظارات")
            rows = [[doc.T(v["full_name_en"], v["full_name_ar"]),
                     f"{doc.sar(v['dental_used_sar'])} / {doc.sar(v['dental_limit_sar'])}", doc.sar(v["dental_remaining_sar"]),
                     f"{doc.sar(v['optical_used_sar'])} / {doc.sar(v['optical_limit_sar'])}", doc.sar(v["optical_remaining_sar"])]
                    for v in views]
            doc.table([("Member", "العضو", 0.28), ("Dental used / limit", "الأسنان: المستخدم / الحد", 0.2),
                       ("Dental left", "المتبقي للأسنان", 0.16), ("Optical used / limit", "النظارات: المستخدم / الحد", 0.2),
                       ("Optical left", "المتبقي للنظارات", 0.16)], rows, size=8.5)
            doc.para(f"Co-pay per visit: {doc.sar(cls.get('copay_per_visit_sar'))}. Out-of-network reimbursement: "
                     f"{_num(cls.get('out_of_network_reimbursement_percent'))}%. Cosmetic dental treatment is not covered.",
                     f"نسبة التحمّل لكل زيارة: {doc.sar(cls.get('copay_per_visit_sar'))}. الاسترداد خارج الشبكة: "
                     f"{_num(cls.get('out_of_network_reimbursement_percent'))}%. علاجات الأسنان التجميلية غير مغطاة.",
                     color=MUTED)
            return doc.finish(), customer["customer_id"]

        if doc_type == "travel_certificate":
            schengen = pol.get("plan_code") == "TRAVEL_SCHENGEN"
            det = plan.get("details") or {}
            cov = _num(cover.get("cover_eur") or det.get("cover_eur"))
            doc.para("This is to certify that the persons named below are insured by Watheeq Cooperative Insurance Co. "
                     "under the travel policy stated, for the period and benefits shown.",
                     "تشهد شركة وثيق للتأمين التعاوني بأن الأشخاص المذكورين أدناه مؤمَّن عليهم بموجب وثيقة السفر "
                     "المبيّنة، للمدة والمنافع الموضحة.")
            doc.kv("Policy number", "رقم الوثيقة", pol["policy_id"], True)
            doc.kv("Plan", "الخطة", doc.T(plan.get("name_en"), plan.get("name_ar")))
            doc.kv("Policyholder", "حامل الوثيقة", _name(doc, customer))
            doc.kv("Destination", "الوجهة", doc.T(cover.get("destination_en"), cover.get("destination_ar")))
            doc.kv("Period of cover", "مدة التغطية",
                   f"{doc.date(pol.get('start_date'))} – {doc.date(pol.get('end_date'))}", True)
            if schengen:
                doc.kv("Trip dates", "تواريخ الرحلة", f"{doc.date(cover.get('trip_start'))} – {doc.date(cover.get('trip_end'))}")
                doc.kv("Extra days after trip", "أيام إضافية بعد الرحلة",
                       doc.T(f"{rules.get('schengen_extra_days')} days", f"{rules.get('schengen_extra_days')} يوماً"))
            doc.kv("Medical cover", "التغطية الطبية", f"EUR {cov:,}" if cov else "—", True)
            doc.kv("Deductible", "مبلغ التحمّل", doc.T("None", "لا يوجد"))
            doc.section("Insured persons", "الأشخاص المؤمَّن عليهم")
            doc.table([("Name", "الاسم", 0.6), ("Relationship", "الصلة", 0.4)],
                      [[doc.T(t.get("name_en"), t.get("name_ar") or t.get("name_en")), _rel(doc, t.get("relationship"))]
                       for t in cover.get("travellers") or []])
            doc.section("Benefits", "المنافع")
            doc.table([("Benefit", "المنفعة", 0.65), ("Limit", "الحد", 0.35)], [
                [doc.T("Emergency medical treatment and hospitalisation", "العلاج الطبي الطارئ والتنويم"), f"EUR {cov:,}" if cov else "—"],
                [doc.T("Medical evacuation and repatriation (incl. repatriation of remains)",
                       "الإخلاء الطبي والإعادة إلى الوطن (بما في ذلك إعادة الجثمان)"), doc.T("Included in medical limit", "ضمن الحد الطبي")],
                [doc.T("Delayed baggage", "تأخر الأمتعة"), doc.sar(det.get("delayed_baggage_limit_sar"))],
                [doc.T("Lost baggage", "فقدان الأمتعة"), doc.sar(det.get("lost_baggage_limit_sar"))]])
            if schengen:
                doc.callout(f"Valid in all Schengen states. Meets the Schengen visa requirement of minimum cover of "
                            f"EUR {int(rules.get('schengen_min_cover_eur') or 30000):,} for emergency medical treatment, "
                            f"hospitalisation and repatriation, for the whole stay plus {rules.get('schengen_extra_days')} days.",
                            f"سارية في جميع دول شنغن. تستوفي متطلبات تأشيرة شنغن بحد أدنى "
                            f"{int(rules.get('schengen_min_cover_eur') or 30000):,} يورو للعلاج الطبي الطارئ والتنويم "
                            f"والإعادة إلى الوطن، طوال مدة الإقامة مع {rules.get('schengen_extra_days')} يوماً إضافية.")
            doc.para("24/7 assistance: +966 11 000 0000 (demo). Verification: scan the code or quote the policy number.",
                     "المساعدة على مدار الساعة: ‎+966 11 000 0000 (تجريبي). للتحقق: امسح الرمز أو اذكر رقم الوثيقة.",
                     color=MUTED)
            doc.qr(f"WATHEEQ|TRAVEL|{pol['policy_id']}|{pol.get('start_date')}|{pol.get('end_date')}|EUR{cov}",
                   doc.x1 - (0 if doc.rtl else 90), max(70, doc.y - 90), 90)
            return doc.finish(), customer["customer_id"]

        # ---- policy schedule --------------------------------------------------
        doc.kv("Policy number", "رقم الوثيقة", pol["policy_id"], True)
        doc.kv("Product", "المنتج", doc.T(pol.get("product"), PRODUCT_AR.get(pol.get("product"))))
        doc.kv("Plan", "الخطة", doc.T(plan.get("name_en"), plan.get("name_ar")))
        doc.kv("Status", "الحالة", _st(doc, pol.get("status")))
        doc.kv("Policyholder", "حامل الوثيقة", _name(doc, customer))
        doc.kv("National ID", "رقم الهوية", mask_national_id(customer.get("national_id")))
        doc.kv("Period", "المدة", f"{doc.date(pol.get('start_date'))} – {doc.date(pol.get('end_date'))}")
        doc.kv("Premium", "القسط", doc.sar(pol.get("premium_paid_sar")))
        doc.para(doc.T(plan.get("cover_summary_en"), plan.get("cover_summary_ar")), color=MUTED)
        product = pol.get("product")
        if product == "Motor":
            v = await sb_get_one("vehicles", {"vehicle_id": f"eq.{cover.get('vehicle_id')}"}, owner=owner) or {}
            doc.section("Vehicle", "المركبة")
            doc.kv("Vehicle", "المركبة", doc.T(f"{v.get('year')} {v.get('make_en')} {v.get('model_en')}",
                                                 f"{v.get('make_ar')} {v.get('model_ar')} {v.get('year')}"))
            doc.kv("Plate", "اللوحة", doc.T(v.get("plate_en"), v.get("plate_ar")))
            doc.kv("Sequence number", "الرقم التسلسلي", v.get("sequence_number"))
            doc.kv("Cover", "نوع التغطية", doc.T("Comprehensive" if cover.get("cover_type") == "Comprehensive" else "Third-party liability",
                                                   "شامل" if cover.get("cover_type") == "Comprehensive" else "ضد الغير"))
            if cover.get("cover_type") == "Comprehensive":
                doc.kv("Repair", "الإصلاح", doc.T("Agency (dealership)" if cover.get("repair") == "Agency" else "Approved workshops",
                                                    "الوكالة" if cover.get("repair") == "Agency" else "الورش المعتمدة"))
                doc.kv("Excess (at fault)", "مبلغ التحمّل (عند الخطأ)", doc.sar(rules.get("motor_excess_sar")))
            if cover.get("upgrade_effective_from"):
                doc.kv("Comprehensive from", "الشامل ساري من", doc.dtm(cover.get("upgrade_effective_from")))
            doc.section("Named drivers", "السائقون المسمّون")
            doc.table([("Name", "الاسم", 0.4), ("Relationship", "الصلة", 0.3), ("ID", "الهوية", 0.3)],
                      [[doc.T(d.get("name_en"), d.get("name_ar") or d.get("name_en")), _rel(doc, d.get("relationship")),
                        f"******{d.get('id_last4') or ''}"] for d in cover.get("drivers") or []])
            if cover.get("addons"):
                doc.section("Extras", "المنافع الإضافية")
                doc.table([("Extra", "المنفعة", 0.4), ("Details", "التفاصيل", 0.6)],
                          [[doc.T(refs["addons"].get(a, {}).get("name_en", a), refs["addons"].get(a, {}).get("name_ar")),
                            doc.T(refs["addons"].get(a, {}).get("limit_text_en"), refs["addons"].get(a, {}).get("limit_text_ar"))]
                           for a in cover["addons"]])
            if cover.get("cancellation"):
                doc.callout(f"Cancelled on {doc.dtm(cover['cancellation'].get('cancelled_at'))} after the vehicle was transferred.",
                            f"أُلغيت في {doc.dtm(cover['cancellation'].get('cancelled_at'))} بعد نقل ملكية المركبة.")
            else:
                doc.callout("Insurance details are sent electronically to the traffic system.",
                            "تُرسل بيانات التأمين إلكترونياً إلى نظام المرور.")
        elif product == "Health":
            cls = refs["classes"].get(cover.get("class_code")) or {}
            members = await sb_get("health_members", {"policy_id": f"eq.{ref}", "order": "member_id.asc"}, owner=owner)
            doc.section("Class and limits", "الفئة والحدود")
            doc.kv("Class", "الفئة", f"{cls.get('class_code')} — {doc.T(cls.get('name_en'), cls.get('name_ar'))}")
            doc.kv("Annual limit per member", "الحد السنوي للعضو", doc.sar(cls.get("annual_limit_sar")))
            doc.kv("Dental / optical", "الأسنان / النظارات", f"{doc.sar(cls.get('dental_limit_sar'))} / {doc.sar(cls.get('optical_limit_sar'))}")
            doc.kv("Co-pay per visit", "التحمّل لكل زيارة", doc.sar(cls.get("copay_per_visit_sar")))
            doc.kv("Out-of-network", "خارج الشبكة", f"{_num(cls.get('out_of_network_reimbursement_percent'))}%")
            doc.section("Members", "الأعضاء")
            doc.table([("Name", "الاسم", 0.36), ("Relationship", "الصلة", 0.2), ("Card number", "رقم البطاقة", 0.26),
                       ("Status", "الحالة", 0.18)],
                      [[doc.T(m.get("full_name_en"), m.get("full_name_ar")), _rel(doc, m.get("relationship")),
                        m.get("card_number"), _st(doc, m.get("status"))] for m in members])
        elif product == "Travel":
            doc.section("Trip", "الرحلة")
            doc.kv("Destination", "الوجهة", doc.T(cover.get("destination_en"), cover.get("destination_ar")))
            doc.kv("Trip", "الرحلة", f"{doc.date(cover.get('trip_start'))} – {doc.date(cover.get('trip_end'))}")
            doc.kv("Medical cover", "التغطية الطبية", f"EUR {_num(cover.get('cover_eur')):,}" if cover.get("cover_eur") else "—")
            doc.table([("Traveller", "المسافر", 0.6), ("Relationship", "الصلة", 0.4)],
                      [[doc.T(t.get("name_en"), t.get("name_ar") or t.get("name_en")), _rel(doc, t.get("relationship"))]
                       for t in cover.get("travellers") or []])
        elif product == "Home":
            doc.section("Insured property", "العقار المؤمَّن")
            doc.kv("Address", "العنوان", doc.T(cover.get("address_en"), cover.get("address_ar")))
            doc.kv("Building sum insured", "مبلغ التأمين للمبنى", doc.sar(cover.get("sum_insured_building_sar")))
            doc.kv("Contents sum insured", "مبلغ التأمين للمحتويات", doc.sar(cover.get("sum_insured_contents_sar")))
            doc.kv("Excess per claim", "التحمّل لكل مطالبة", doc.sar(cover.get("excess_sar") or rules.get("home_excess_sar")))
            perils = (plan.get("details") or {}).get("perils") or []
            if perils:
                doc.kv("Perils covered", "الأخطار المغطاة", ", ".join(perils))
        return doc.finish(), customer["customer_id"]

    if doc_type == "health_card":
        m = await _load("health_members", "member_id", ref, owner, "member_not_found", "member")
        pol = await _load("policies", "policy_id", m["policy_id"], owner, "policy_not_found", "policy")
        customer = await _load("customers", "customer_id", m["customer_id"], owner, "customer_not_found", "customer")
        doc = PdfDoc(lang, title_en, title_ar, m.get("card_number") or ref)
        _health_card_page(doc, m, pol, refs["classes"].get(m.get("class_code")) or {}, customer)
        return doc.finish(), customer["customer_id"]

    if doc_type in ("booking_confirmation", "rental_voucher"):
        b = await _load("bookings", "booking_id", ref, owner, "booking_not_found", "booking")
        if doc_type == "rental_voucher" and b.get("booking_type") != "Rental Car":
            raise err(409, "wrong_booking_type", f"{ref} is a {b.get('booking_type')} booking, not a rental")
        place = refs["providers"].get(b.get("provider_id")) or refs["centres"].get(b.get("centre_id")) or {}
        customer = (await sb_get_one("customers", {"customer_id": f"eq.{b['customer_id']}"}, owner=owner)
                    if b.get("customer_id") else None) or {}
        det = b.get("details") or {}
        doc = PdfDoc(lang, title_en, title_ar, ref)
        if doc_type == "rental_voucher":
            claim = await sb_get_one("claims", {"claim_id": f"eq.{b.get('claim_id')}"}, owner=owner) or {}
            doc.kv("Voucher", "رقم القسيمة", ref, True)
            doc.kv("Customer", "العميل", _name(doc, customer))
            doc.kv("Claim", "المطالبة", b.get("claim_id"))
            doc.kv("Rental partner", "شريك التأجير", doc.T(place.get("name_en"), place.get("name_ar")))
            doc.kv("Branch address", "عنوان الفرع", doc.T(place.get("address_en"), place.get("address_ar")))
            doc.kv("Branch phone", "هاتف الفرع", place.get("phone"))
            doc.kv("Pick-up", "الاستلام", doc.dtm(b.get("start_at")), True)
            doc.kv("Return by", "الإرجاع قبل", doc.dtm(b.get("end_at")))
            doc.kv("Days", "عدد الأيام", det.get("days"))
            doc.kv("Daily rate (paid by Watheeq)", "السعر اليومي (تدفعه وثيق)", doc.sar(det.get("daily_rate_sar")))
            doc.kv("Total covered", "الإجمالي المغطّى", doc.sar(det.get("total_sar")), True)
            rent = claim.get("rental") or {}
            doc.kv("Benefit limit per accident", "حد المنفعة لكل حادث", doc.sar(rent.get("limit_sar")))
            doc.callout("Bring your national ID and driving licence. Fuel, traffic fines and extra days beyond the "
                        "voucher are the driver's responsibility. Message us on WhatsApp to extend while you are under the limit.",
                        "أحضر الهوية الوطنية ورخصة القيادة. الوقود والمخالفات والأيام الإضافية خارج القسيمة على السائق. "
                        "راسلنا عبر واتساب للتمديد ما دمت ضمن الحد.")
        else:
            doctor = refs["doctors"].get(b.get("doctor_id")) or {}
            doc.kv("Booking", "رقم الحجز", ref, True)
            doc.kv("Type", "النوع", doc.T(b.get("booking_type"), {"Inspection": "معاينة المركبة",
                                                                   "Hospital Appointment": "موعد مستشفى",
                                                                   "Home Inspection": "معاينة المنزل",
                                                                   "Dental": "موعد أسنان"}.get(b.get("booking_type"))))
            who = customer
            if b.get("member_id"):
                who = await sb_get_one("health_members", {"member_id": f"eq.{b['member_id']}"}, owner=owner) or customer
            if not b.get("customer_id"):  # third-party claimant (I-13)
                doc.kv("Claimant", "صاحب المطالبة", det.get("claimant_name_en"))
            else:
                doc.kv("Name", "الاسم", _name(doc, who))
            doc.kv("When", "الموعد", doc.dtm(b.get("start_at")), True)
            if b.get("booking_type") == "Home Inspection" and b.get("end_at"):
                doc.kv("Window ends", "نهاية النافذة", doc.dtm(b.get("end_at")))
            doc.kv("Place", "المكان", doc.T(place.get("name_en"), place.get("name_ar")))
            doc.kv("Address", "العنوان", doc.T(place.get("address_en"), place.get("address_ar")))
            doc.kv("Phone", "الهاتف", place.get("phone"))
            if doctor:
                doc.kv("Doctor", "الطبيب", doc.T(doctor.get("name_en"), doctor.get("name_ar")))
                doc.kv("Specialty", "التخصص", doc.T(doctor.get("specialty_en"), doctor.get("specialty_ar")))
            if b.get("patient_pays_sar") is not None and b.get("booking_type") in ("Hospital Appointment", "Dental"):
                doc.kv("You pay", "المبلغ الذي تدفعه", doc.sar(b.get("patient_pays_sar")), True)
            if b.get("claim_id"):
                doc.kv("Claim", "المطالبة", b.get("claim_id"))
            bring = {"Inspection": ("Bring your national ID, the vehicle registration and the Najm accident report.",
                                    "أحضر الهوية الوطنية واستمارة المركبة وتقرير نجم."),
                     "Hospital Appointment": ("Show your Watheeq card (digital is fine) and ID at reception; pay only the co-pay.",
                                              "اعرض بطاقة وثيق (الرقمية مقبولة) والهوية عند الاستقبال، وادفع نسبة التحمّل فقط."),
                     "Dental": ("Show your Watheeq card at reception. Cleanings are covered from the dental limit.",
                                "اعرض بطاقة وثيق عند الاستقبال. التنظيف مغطّى من حد الأسنان."),
                     "Home Inspection": ("Keep damaged items in place until the inspector has seen them.",
                                         "احتفظ بالأغراض المتضررة في مكانها حتى يراها المعاين.")}.get(b.get("booking_type"))
            if bring:
                doc.callout(bring[0], bring[1])
        return doc.finish(), b.get("customer_id")

    if doc_type in ("claim_summary", "accident_pack"):
        cl = await _load("claims", "claim_id", ref, owner, "claim_not_found", "claim")
        customer = (await sb_get_one("customers", {"customer_id": f"eq.{cl.get('customer_id')}"}, owner=owner)
                    if cl.get("customer_id") else None) or {}
        doc = PdfDoc(lang, title_en, title_ar, ref)
        ec = enrich_claim(cl, {"rules": rules})
        if doc_type == "claim_summary":
            doc.kv("Claim", "المطالبة", ref, True)
            doc.kv("Type", "النوع", doc.T(cl.get("claim_type"), CLAIM_TYPE_AR.get(cl.get("claim_type"))))
            doc.kv("Status", "الحالة", _st(doc, cl.get("status")), True)
            if customer:
                doc.kv("Customer", "العميل", _name(doc, customer))
            elif cl.get("claimant"):
                doc.kv("Claimant", "صاحب المطالبة", (cl.get("claimant") or {}).get("name_en"))
            doc.kv("Policy", "الوثيقة", cl.get("policy_id"))
            doc.kv("Opened", "تاريخ الفتح", doc.dtm(cl.get("opened_at")))
            doc.kv("Days open", "أيام منذ الفتح", ec.get("days_open"))
            if cl.get("najm_report_number"):
                doc.kv("Najm report", "تقرير نجم", cl.get("najm_report_number"))
            doc.kv("Description", "الوصف", doc.T(cl.get("description_en"), cl.get("description_ar") or cl.get("description_en")))
            if cl.get("fault_percent") is not None:
                doc.kv("Fault", "نسبة الخطأ", f"{cl.get('fault_percent')}%")
            doc.kv("You pay (excess)", "مبلغ التحمّل", doc.sar(cl.get("customer_pays_sar")))
            if cl.get("approved_amount_sar") is not None:
                doc.kv("Approved amount", "المبلغ المعتمد", doc.sar(cl.get("approved_amount_sar")), True)
            if cl.get("amounts"):
                a = cl["amounts"]
                if a.get("payable") is not None:
                    doc.kv("Invoice total", "إجمالي الفاتورة", doc.sar(a.get("invoice_total")))
                    doc.kv("Reimbursement", "نسبة الاسترداد", f"{a.get('percent')}%")
                    doc.kv("Payable", "المبلغ المستحق", doc.sar(a.get("payable")), True)
                if a.get("incident"):
                    doc.kv("Incident", "الحادثة", a.get("incident"))
                    doc.kv("Delayed / lost baggage limit", "حد تأخر / فقدان الأمتعة",
                           f"{doc.sar(a.get('delayed_baggage_limit_sar'))} / {doc.sar(a.get('lost_baggage_limit_sar'))}")
            if cl.get("valuation"):
                v = cl["valuation"]
                doc.section("Valuation", "التقييم")
                rows = [[doc.T("Market price", "سعر السوق"), doc.sar(v.get("market_price"))]]
                rows += [[doc.T(d.get("label_en"), d.get("label_ar")), "− " + doc.sar(d.get("amount"))]
                         for d in v.get("deductions") or []]
                rows.append([doc.T("Offered amount", "المبلغ المعروض"), doc.sar(v.get("offered_amount"))])
                doc.table([("Item", "البند", 0.7), ("Amount", "المبلغ", 0.3)], rows)
            if cl.get("rental"):
                r = cl["rental"]
                doc.kv("Replacement car", "السيارة البديلة",
                       doc.T(f"{r.get('days') or 0} days used, {doc.sar(r.get('used_sar'))} of {doc.sar(r.get('limit_sar'))}",
                             f"{r.get('days') or 0} يوم، {doc.sar(r.get('used_sar'))} من {doc.sar(r.get('limit_sar'))}"))
            if cl.get("rejection_reason_en"):
                doc.callout(doc.T("Reason for rejection: " + (cl.get("rejection_reason_en") or ""),
                                  "سبب الرفض: " + (cl.get("rejection_reason_ar") or cl.get("rejection_reason_en") or "")))
            doc.section("Documents", "المستندات")
            missing = cl.get("missing_documents") or []
            got = cl.get("documents") or []
            rows = [[doc.T(DOC_LABELS.get(d.get("code"), (d.get("code"),))[0] if d.get("code") in DOC_LABELS
                           else str(d.get("code")).replace("_", " "),
                           DOC_LABELS.get(d.get("code"), (None, None))[1]), doc.T("Received", "مستلم"),
                     doc.dtm(d.get("received_at"))] for d in got]
            rows += [[doc.T(m.get("label_en"), m.get("label_ar")), doc.T("MISSING", "ناقص"), "—"] for m in missing]
            doc.table([("Document", "المستند", 0.5), ("Status", "الحالة", 0.2), ("Received", "تاريخ الاستلام", 0.3)], rows)
            if ec.get("payment_due_by"):
                txt = doc.T(f"Payment due by {doc.date(ec['payment_due_by'])} ({rules.get('claim_payment_days')} days from complete documents)",
                            f"موعد الصرف قبل {doc.date(ec['payment_due_by'])} ({rules.get('claim_payment_days')} يوماً من اكتمال المستندات)")
                if ec.get("overdue"):
                    txt += doc.T(f" — OVERDUE by {ec['overdue_days']} days", f" — متأخر {ec['overdue_days']} يوماً")
                doc.callout(txt, color=GOLD)
            return doc.finish(), cl.get("customer_id")

        # accident_pack: what the customer brings to the inspection (I-09)
        if not cl.get("najm_report_number"):
            raise err(409, "no_accident_report", f"{ref} has no Najm report attached")
        rep = await sb_get_one("najm_reports", {"report_number": f"eq.{cl['najm_report_number']}"}, owner=owner) or {}
        veh = (await sb_get_one("vehicles", {"vehicle_id": f"eq.{cl.get('vehicle_id')}"}, owner=owner)
               if cl.get("vehicle_id") else None) or {}
        doc.para("Show this pack at the inspection centre. It summarises the three documents they ask for.",
                 "اعرض هذا الملف في مركز المعاينة. يلخّص المستندات الثلاثة المطلوبة.", color=MUTED)
        doc.section("1. Identity", "1. الهوية")
        doc.kv("Name", "الاسم", _name(doc, customer))
        doc.kv("National ID", "رقم الهوية", mask_national_id(customer.get("national_id")))
        doc.kv("Date of birth", "تاريخ الميلاد", doc.date(customer.get("dob")))
        doc.kv("Mobile", "الجوال", customer.get("phone"))
        doc.section("2. Vehicle registration", "2. استمارة المركبة")
        doc.kv("Vehicle", "المركبة", doc.T(f"{veh.get('year')} {veh.get('make_en')} {veh.get('model_en')}",
                                             f"{veh.get('make_ar')} {veh.get('model_ar')} {veh.get('year')}"))
        doc.kv("Plate", "اللوحة", doc.T(veh.get("plate_en"), veh.get("plate_ar")))
        doc.kv("Sequence number", "الرقم التسلسلي", veh.get("sequence_number"))
        doc.kv("Colour", "اللون", doc.T(veh.get("color_en"), veh.get("color_ar")))
        doc.kv("Registration valid to", "صلاحية الاستمارة", doc.date(veh.get("registration_expiry")))
        doc.kv("Insurance", "التأمين", f"{cl.get('policy_id')} — Watheeq")
        doc.section("3. Najm accident report", "3. تقرير نجم")
        doc.kv("Report number", "رقم التقرير", rep.get("report_number"), True)
        doc.kv("Date and time", "التاريخ والوقت", doc.dtm(rep.get("accident_at")))
        doc.kv("Location", "الموقع", doc.T(rep.get("location_en"), rep.get("location_ar")))
        doc.kv("Report type", "نوع التقرير", rep.get("report_type"))
        doc.kv("Damage", "الأضرار", doc.T(rep.get("damage_en"), rep.get("damage_ar")))
        doc.table([("Party", "الطرف", 0.1), ("Driver", "السائق", 0.34), ("Plate", "اللوحة", 0.18),
                   ("Insurer", "شركة التأمين", 0.24), ("Fault", "الخطأ", 0.14)],
                  [[p.get("party"), doc.T(p.get("name_en"), p.get("name_ar")), p.get("plate_en"), p.get("insurer_en"),
                    f"{_num(p.get('fault_percent'))}%"] for p in rep.get("parties") or []])
        doc.kv("Claim", "المطالبة", ref)
        return doc.finish(), cl.get("customer_id")

    if doc_type == "payment_receipt":
        pr = await _load("payment_requests", "payment_id", ref, owner, "payment_not_found", "payment")
        if pr.get("status") != "Paid":
            raise err(409, "not_paid", f"{ref} is {pr.get('status')} — a receipt exists only after payment",
                      status=pr.get("status"))
        customer = await sb_get_one("customers", {"customer_id": f"eq.{pr.get('customer_id')}"}, owner=owner) or {}
        q = (await sb_get_one("quotes", {"quote_id": f"eq.{pr.get('reference_id')}"}, owner=owner)
             if pr.get("reference_type") == "Quote" else None) or {}
        doc = PdfDoc(lang, title_en, title_ar, ref)
        doc.kv("Receipt for", "إيصال عن", ref, True)
        doc.kv("Customer", "العميل", _name(doc, customer))
        doc.kv("Description", "الوصف", doc.T(pr.get("description_en"), pr.get("description_ar")))
        doc.kv("Reference", "المرجع", f"{pr.get('reference_type')} {pr.get('reference_id')}")
        if (q.get("params") or {}).get("applied_policy_id"):
            doc.kv("Policy", "الوثيقة", q["params"]["applied_policy_id"])
        doc.kv("Paid on", "تاريخ الدفع", doc.dtm(pr.get("paid_at")))
        doc.kv("Method", "طريقة الدفع", pr.get("method"))
        if q.get("breakdown"):
            doc.table([("Item", "البند", 0.7), ("Amount", "المبلغ", 0.3)],
                      [[doc.T(b.get("label_en"), b.get("label_ar")), doc.sar(b.get("amount"))] for b in q["breakdown"]])
        doc.kv("Amount paid", "المبلغ المدفوع", doc.sar(pr.get("amount_sar")), True)
        doc.para("Premiums include VAT where applicable. Demonstration document — no real payment was taken.",
                 "الأقساط شاملة لضريبة القيمة المضافة حيثما ينطبق. مستند تجريبي — لم تُحصَّل أي دفعة فعلية.", color=MUTED)
        return doc.finish(), pr.get("customer_id")

    if doc_type == "refund_notice":
        rf = await _load("refunds", "refund_id", ref, owner, "refund_not_found", "refund")
        customer = await sb_get_one("customers", {"customer_id": f"eq.{rf.get('customer_id')}"}, owner=owner) or {}
        pol = await sb_get_one("policies", {"policy_id": f"eq.{rf.get('policy_id')}"}, owner=owner) or {}
        q = await sb_get_one("quotes", {"policy_id": f"eq.{rf.get('policy_id')}", "quote_type": "eq.cancel_refund",
                                        "status": "eq.Applied", "order": "created_at.desc"}, owner=owner) or {}
        doc = PdfDoc(lang, title_en, title_ar, ref)
        doc.kv("Refund", "الاسترداد", ref, True)
        doc.kv("Customer", "العميل", _name(doc, customer))
        doc.kv("Policy", "الوثيقة", f"{rf.get('policy_id')} — {_st(doc, pol.get('status'))}")
        if q.get("breakdown"):
            doc.table([("Calculation", "طريقة الحساب", 0.7), ("Amount", "المبلغ", 0.3)],
                      [[doc.T(b.get("label_en"), b.get("label_ar")), doc.sar(b.get("amount"))] for b in q["breakdown"]])
        doc.kv("Amount", "المبلغ", doc.sar(rf.get("amount_sar")), True)
        doc.kv("To account", "إلى الحساب", rf.get("iban_masked"))
        doc.kv("Status", "الحالة", _st(doc, rf.get("status")))
        doc.kv("Expected by", "متوقع قبل", doc.date(rf.get("expected_by")), True)
        doc.callout("The refund is sent within three working days (Sunday–Thursday) to the IBAN in the policyholder's name.",
                    "يُحوَّل المبلغ خلال ثلاثة أيام عمل (الأحد–الخميس) إلى الآيبان باسم حامل الوثيقة.")
        return doc.finish(), rf.get("customer_id")

    raise err(400, "invalid_type", f"Unknown document type {doc_type}")


@app.get("/document")
async def get_document(
    type: str = Query(..., description="policy | health_card | health_cards_family | travel_certificate | "
                                       "limits_sheet | booking_confirmation | rental_voucher | claim_summary | "
                                       "payment_receipt | refund_notice | accident_pack"),
    ref: str = Query(...),
    lang: str = Query("en", description="en | ar"),
    caller_phone: Optional[str] = Query(None, description="Demo tenant routing — WhatsApp sender number"),
):
    """Render a branded PDF, upload to Storage (upsert), return a 1 h signed
    URL + the fields send_whatsapp_media needs (media_type 5 = document)."""
    owner = await resolve_owner(caller_phone)
    t = (type or "").strip().lower()
    if t not in DOC_TYPES:
        raise err(400, "invalid_type", f"type must be one of {', '.join(DOC_TYPES)}", allowed=list(DOC_TYPES))
    lg = "ar" if (lang or "").strip().lower().startswith("ar") else "en"
    ref = (ref or "").strip()
    if not ref:
        raise err(400, "ref_required", "ref is required")
    pdf, customer_id = await _render(t, ref, lg, owner)
    safe_ref = re.sub(r"[^A-Za-z0-9_\-]", "_", ref)
    path = f"{owner}/{t}/{safe_ref}-{lg}.pdf"
    await storage_upload(path, pdf, "application/pdf")
    url = await storage_sign(path, 3600)
    file_name = f"Watheeq-{t.replace('_', '-')}-{safe_ref}-{lg}.pdf"
    await log_agent_action(customer_id, "Document Issued",
                           f"Sent {_doc_label(t, ref)} {ref} (PDF{', Arabic' if lg == 'ar' else ''})",
                           {"type": t, "ref": ref, "lang": lg, "path": path, "bytes": len(pdf)},
                           owner=owner, reference_id=ref)
    return {
        "ok": True,
        "download_url": url,
        "file_name": file_name,
        "content_type": "application/pdf",
        "media_type": 5,
        "type": t,
        "ref": ref,
        "lang": lg,
        "size_bytes": len(pdf),
        "expires_in_seconds": 3600,
        "arabic_rendering": "shaped" if (lg == "ar" and _FONTS["ok"]) else ("unavailable" if lg == "ar" else None),
    }
