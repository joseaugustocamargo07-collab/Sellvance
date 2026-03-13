"""
Sellvance - Sync Base Infrastructure
Token management, HTTP helpers, sync orchestration.
"""

import json
import time
import threading
import traceback
import urllib.request
import urllib.error
import urllib.parse
from database import get_db

# Thread locks per org+platform to prevent concurrent syncs
_sync_locks = {}
_locks_lock = threading.Lock()


def _get_lock(org_id, platform):
    key = f"{org_id}:{platform}"
    with _locks_lock:
        if key not in _sync_locks:
            _sync_locks[key] = threading.Lock()
        return _sync_locks[key]


def get_valid_token(org_id, platform):
    """Load token from DB, refresh if expired, return access_token string."""
    from oauth_manager import get_integration, refresh_access_token, OAUTH_APPS

    integration = get_integration(org_id, platform)
    if not integration:
        print(f"[sync] get_valid_token: no integration found for org={org_id} platform={platform}")
        return None
    
    status = integration.get('status', '')
    print(f"[sync] get_valid_token: integration found, status='{status}', account='{integration.get('account_name','')}'")
    
    if status != 'connected':
        print(f"[sync] get_valid_token: status is not 'connected', returning None")
        return None

    config = integration.get('config', {})
    access_token = config.get('access_token', '')
    refresh_token = config.get('refresh_token', '')
    
    print(f"[sync] get_valid_token: access_token={'YES (len=' + str(len(access_token)) + ')' if access_token else 'EMPTY'}, refresh_token={'YES' if refresh_token else 'EMPTY'}")

    if not access_token:
        print(f"[sync] get_valid_token: no access_token, trying refresh...")
        if refresh_token:
            new_token = force_refresh_token(org_id, platform)
            if new_token:
                return new_token
        return None

    return access_token


def force_refresh_token(org_id, platform):
    """Force token refresh and return new access_token."""
    from oauth_manager import get_integration, refresh_access_token, save_integration

    integration = get_integration(org_id, platform)
    if not integration:
        return None

    config = integration.get('config', {})
    refresh_token = config.get('refresh_token', '')
    if not refresh_token:
        return None

    new_data, error = refresh_access_token(platform, refresh_token)
    if error or not new_data:
        print(f"[sync] Token refresh failed for {platform}: {error}")
        # Mark integration as needing reconnect
        db = get_db()
        db.execute("UPDATE api_integrations SET status='token_expired' WHERE org_id=? AND platform=?",
                   (org_id, platform))
        db.commit()
        db.close()
        return None

    # Save new tokens
    account_info = {'id': config.get('user_id', ''), 'name': integration.get('account_name', '')}
    save_integration(org_id, platform, new_data, account_info)
    return new_data.get('access_token', '')


def api_request(url, headers=None, method='GET', data=None, retries=2, timeout=15):
    """HTTP request wrapper with retry, JSON parsing, and error handling."""
    hdrs = {'User-Agent': 'Sellvance/1.0'}
    if headers:
        hdrs.update(headers)

    body = None
    if data:
        if isinstance(data, dict):
            body = json.dumps(data).encode()
            hdrs['Content-Type'] = 'application/json'
        elif isinstance(data, bytes):
            body = data
        else:
            body = str(data).encode()

    last_error = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, data=body, headers=hdrs, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if not raw:
                    return {}
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            status = e.code
            err_body = e.read().decode()[:500]
            if status == 401:
                raise AuthError(f"401 Unauthorized: {err_body}")
            if status == 429:
                # Rate limited - wait and retry
                retry_after = int(e.headers.get('Retry-After', '5'))
                time.sleep(min(retry_after, 10))
                last_error = f"429 Rate Limited"
                continue
            if status >= 500 and attempt < retries:
                time.sleep(2 ** attempt)
                last_error = f"{status}: {err_body}"
                continue
            raise ApiError(f"HTTP {status}: {err_body}")
        except Exception as e:
            if attempt < retries:
                time.sleep(1)
                last_error = str(e)
                continue
            raise ApiError(f"Request failed: {e}")

    raise ApiError(f"Max retries exceeded. Last error: {last_error}")


class AuthError(Exception):
    """Token expired or invalid."""
    pass

class ApiError(Exception):
    """General API error."""
    pass


def is_stale(org_id, platform, sync_type='full', max_age_minutes=60):
    """Check if data needs re-sync. Returns True if stale or no prior sync."""
    db = get_db()
    row = db.execute(
        """SELECT finished_at FROM sync_log
           WHERE org_id=? AND platform=? AND sync_type=? AND status='success'
           ORDER BY finished_at DESC LIMIT 1""",
        (org_id, platform, sync_type)
    ).fetchone()
    db.close()

    if not row or not row['finished_at']:
        return True

    from datetime import datetime, timedelta
    try:
        last = datetime.fromisoformat(row['finished_at'])
        return datetime.now() - last > timedelta(minutes=max_age_minutes)
    except Exception:
        return True


def log_sync(org_id, platform, sync_type, status, records_synced=0, error_message=''):
    """Record sync attempt in sync_log table."""
    db = get_db()
    from datetime import datetime
    now = datetime.now().isoformat()
    db.execute(
        """INSERT INTO sync_log (org_id, platform, sync_type, status, records_synced, error_message, started_at, finished_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (org_id, platform, sync_type, status, records_synced, error_message[:500], now, now)
    )
    db.commit()
    db.close()


def get_last_sync_info(org_id, platform):
    """Get last sync timestamp and status for display."""
    db = get_db()
    row = db.execute(
        """SELECT status, finished_at, records_synced, error_message FROM sync_log
           WHERE org_id=? AND platform=?
           ORDER BY finished_at DESC LIMIT 1""",
        (org_id, platform)
    ).fetchone()
    db.close()
    if row:
        return dict(row)
    return None


def run_sync_if_needed(org_id, platform, sync_func, sync_type='full', max_age=60):
    """Check staleness, acquire lock, run sync, log result. Returns True if sync ran."""
    if not is_stale(org_id, platform, sync_type, max_age):
        return False

    lock = _get_lock(org_id, platform)
    if not lock.acquire(blocking=False):
        # Another thread is already syncing
        return False

    try:
        records = sync_func(org_id)
        log_sync(org_id, platform, sync_type, 'success', records_synced=records or 0)
        return True
    except AuthError as e:
        # Try token refresh once
        new_token = force_refresh_token(org_id, platform)
        if new_token:
            try:
                records = sync_func(org_id)
                log_sync(org_id, platform, sync_type, 'success', records_synced=records or 0)
                return True
            except Exception as e2:
                log_sync(org_id, platform, sync_type, 'error', error_message=str(e2))
                return False
        log_sync(org_id, platform, sync_type, 'auth_error', error_message=str(e))
        return False
    except Exception as e:
        traceback.print_exc()
        log_sync(org_id, platform, sync_type, 'error', error_message=str(e))
        return False
    finally:
        lock.release()
