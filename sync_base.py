"""
Sellvance -- Sync Infrastructure
Provides token management, HTTP helpers, staleness checks, and sync orchestration
for all marketplace and ads platform integrations.
"""

import json
import time
import logging
import threading
import urllib.request
import urllib.error
from database import get_db
from oauth_manager import get_integration, refresh_access_token, save_integration

logger = logging.getLogger(__name__)


class AuthError(RuntimeError):
    """Raised when an API returns 401/403 indicating invalid or expired credentials."""
    pass


# ── Per-org+platform locks to prevent concurrent syncs ────────────────────────
_sync_locks = {}
_locks_lock = threading.Lock()


def _get_lock(org_id, platform):
    """Return a per-org+platform threading.Lock, creating it on first use."""
    key = (org_id, platform)
    with _locks_lock:
        if key not in _sync_locks:
            _sync_locks[key] = threading.Lock()
        return _sync_locks[key]


# ── Token management ─────────────────────────────────────────────────────────

def get_valid_token(org_id, platform):
    """Load the access token for *org_id*/*platform*, refreshing it when expired.

    Returns the access_token string or raises RuntimeError if the integration
    is missing, disconnected, or the refresh fails.
    """
    integration = get_integration(org_id, platform)
    if not integration or integration.get('status') != 'connected':
        raise RuntimeError(f"Integration {platform} not connected for org {org_id}")

    config = integration.get('config', {})
    access_token = config.get('access_token', '')
    refresh_token = config.get('refresh_token', '')
    expires_in = int(config.get('expires_in', 3600))

    if not access_token:
        raise RuntimeError(f"No access_token stored for {platform} org {org_id}")

    # Check expiry: last_sync timestamp + expires_in vs now
    last_sync = integration.get('last_sync', '')
    token_expired = False
    if last_sync and refresh_token:
        try:
            from datetime import datetime
            last_dt = datetime.strptime(last_sync, '%Y-%m-%d %H:%M:%S')
            elapsed = (datetime.utcnow() - last_dt).total_seconds()
            # Refresh 5 minutes before actual expiry to avoid race conditions
            if elapsed >= (expires_in - 300):
                token_expired = True
        except (ValueError, TypeError):
            token_expired = True

    if token_expired and refresh_token:
        new_data, err = refresh_access_token(platform, refresh_token)
        if err:
            raise RuntimeError(f"Token refresh failed for {platform}: {err}")
        # Merge new tokens into existing config and persist
        config['access_token'] = new_data.get('access_token', access_token)
        if new_data.get('refresh_token'):
            config['refresh_token'] = new_data['refresh_token']
        if new_data.get('expires_in'):
            config['expires_in'] = new_data['expires_in']
        save_integration(org_id, platform, config)
        access_token = config['access_token']

    return access_token


# ── HTTP helper ──────────────────────────────────────────────────────────────

def api_request(url, headers=None, method='GET', data=None, retries=2, timeout=15):
    """Make an HTTP request and return parsed JSON.

    Retries on 5xx errors up to *retries* times with exponential back-off.
    Raises RuntimeError on 4xx or after exhausting retries.
    """
    if headers is None:
        headers = {}

    body = None
    if data is not None:
        if isinstance(data, (dict, list)):
            body = json.dumps(data).encode('utf-8')
            headers.setdefault('Content-Type', 'application/json')
        elif isinstance(data, bytes):
            body = data
        else:
            body = str(data).encode('utf-8')

    last_error = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if not raw:
                    return {}
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            status = e.code
            err_body = ''
            try:
                err_body = e.read().decode('utf-8', errors='replace')[:500]
            except Exception:
                pass

            if 400 <= status < 500:
                raise RuntimeError(f"HTTP {status} from {url}: {err_body}")

            # 5xx -- retry
            last_error = f"HTTP {status} from {url}: {err_body}"
            logger.warning("Retry %d/%d for %s: %s", attempt + 1, retries, url, last_error)
            if attempt < retries:
                time.sleep(2 ** attempt)
        except Exception as exc:
            last_error = str(exc)
            logger.warning("Retry %d/%d for %s: %s", attempt + 1, retries, url, last_error)
            if attempt < retries:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"Request to {url} failed after {retries + 1} attempts: {last_error}")


# ── Staleness check ──────────────────────────────────────────────────────────

def _ensure_sync_log_table():
    """Create the sync_log table if it does not exist."""
    db = get_db()
    try:
        db.execute('''
            CREATE TABLE IF NOT EXISTS sync_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id          INTEGER NOT NULL,
                platform        TEXT NOT NULL,
                sync_type       TEXT NOT NULL,
                status          TEXT NOT NULL,
                records_synced  INTEGER DEFAULT 0,
                error_message   TEXT DEFAULT '',
                created_at      TEXT DEFAULT (datetime('now'))
            )
        ''')
        db.commit()
    finally:
        db.close()


# Ensure table exists on module import
_ensure_sync_log_table()


def is_stale(org_id, platform, sync_type, max_age_minutes=60):
    """Return True if the last successful sync is older than *max_age_minutes* or missing."""
    db = get_db()
    try:
        row = db.execute('''
            SELECT created_at FROM sync_log
            WHERE org_id = ? AND platform = ? AND sync_type = ? AND status = 'success'
            ORDER BY created_at DESC LIMIT 1
        ''', (org_id, platform, sync_type)).fetchone()
    finally:
        db.close()

    if not row:
        return True

    try:
        from datetime import datetime, timedelta
        last = datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S')
        return datetime.utcnow() - last > timedelta(minutes=max_age_minutes)
    except (ValueError, TypeError):
        return True


def log_sync(org_id, platform, sync_type, status, records_synced=0, error_message=''):
    """Insert a row into sync_log."""
    db = get_db()
    try:
        db.execute('''
            INSERT INTO sync_log (org_id, platform, sync_type, status, records_synced, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (org_id, platform, sync_type, status, records_synced, error_message))
        db.commit()
    finally:
        db.close()


def get_last_sync_info(org_id, platform):
    """Return the last sync_log row for *org_id*/*platform* (any status), or None."""
    db = get_db()
    try:
        row = db.execute('''
            SELECT * FROM sync_log
            WHERE org_id = ? AND platform = ?
            ORDER BY created_at DESC LIMIT 1
        ''', (org_id, platform)).fetchone()
        return dict(row) if row else None
    except Exception:
        return None
    finally:
        db.close()


# ── Orchestrator ─────────────────────────────────────────────────────────────

def run_sync_if_needed(org_id, platform, sync_func, sync_type='full', max_age=60):
    """Run *sync_func(org_id)* if the data is stale, with locking and logging.

    Parameters
    ----------
    org_id : int
    platform : str
    sync_func : callable(org_id) -> int
        Should return the number of records synced.
    sync_type : str
        Label stored in sync_log (e.g. 'full', 'orders', 'campaigns').
    max_age : int
        Maximum age in minutes before data is considered stale.

    Returns
    -------
    dict with keys 'synced' (bool), 'records' (int), 'error' (str or None).
    """
    if not is_stale(org_id, platform, sync_type, max_age_minutes=max_age):
        return {'synced': False, 'records': 0, 'error': None}

    lock = _get_lock(org_id, platform)
    acquired = lock.acquire(blocking=False)
    if not acquired:
        logger.info("Sync already in progress for org=%s platform=%s", org_id, platform)
        return {'synced': False, 'records': 0, 'error': 'sync_in_progress'}

    try:
        records = sync_func(org_id)
        if records and records > 0:
            log_sync(org_id, platform, sync_type, 'success', records_synced=records)
        return {'synced': True, 'records': records or 0, 'error': None}
    except Exception as exc:
        error_msg = str(exc)[:500]
        logger.exception("Sync failed for org=%s platform=%s: %s", org_id, platform, error_msg)
        log_sync(org_id, platform, sync_type, 'error', error_message=error_msg)
        return {'synced': False, 'records': 0, 'error': error_msg}
    finally:
        lock.release()
