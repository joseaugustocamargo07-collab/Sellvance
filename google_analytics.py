# google_analytics.py — Integracao com Google Analytics 4 (GA4)
# Puxa metricas de trafego, conversoes, funil e sources via GA4 Data API.
#
# OAuth: usa o mesmo flow de Google que ja existe no oauth_manager.py
# API: Google Analytics Data API v1beta
#      https://developers.google.com/analytics/devguides/reporting/data/v1
#
# Requer:
#   - Google OAuth com scope analytics.readonly
#   - GA4 Property ID do cliente (ex: 123456789)

import json
import urllib.request
from database import get_db


GA4_API_BASE = 'https://analyticsdata.googleapis.com/v1beta'


def ensure_tables():
    """Bootstrap das tabelas de GA4."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS ga4_config (
            org_id          INTEGER PRIMARY KEY,
            property_id     TEXT NOT NULL,
            access_token    TEXT,
            refresh_token   TEXT,
            last_sync       TEXT,
            status          TEXT DEFAULT 'pending',
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS ga4_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            period          TEXT NOT NULL,
            sessions        INTEGER DEFAULT 0,
            users           INTEGER DEFAULT 0,
            new_users       INTEGER DEFAULT 0,
            pageviews       INTEGER DEFAULT 0,
            bounce_rate     REAL DEFAULT 0,
            avg_session_sec REAL DEFAULT 0,
            conversions     INTEGER DEFAULT 0,
            revenue         REAL DEFAULT 0,
            captured_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS ga4_sources (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            period          TEXT NOT NULL,
            source          TEXT,
            medium          TEXT,
            sessions        INTEGER DEFAULT 0,
            users           INTEGER DEFAULT 0,
            conversions     INTEGER DEFAULT 0,
            revenue         REAL DEFAULT 0,
            captured_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS ga4_pages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            period          TEXT NOT NULL,
            page_path       TEXT,
            pageviews       INTEGER DEFAULT 0,
            avg_time_sec    REAL DEFAULT 0,
            bounce_rate     REAL DEFAULT 0,
            captured_at     TEXT DEFAULT (datetime('now'))
        );
    ''')
    db.commit()
    db.close()


def _ga4_api(property_id, access_token, endpoint, body):
    """Chamada generica pra GA4 Data API."""
    url = f'{GA4_API_BASE}/properties/{property_id}:{endpoint}'
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method='POST', headers={
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        err = e.read().decode('utf-8', errors='replace')[:500]
        return {'error': e.code, 'message': err}
    except Exception as e:
        return {'error': str(e)[:200]}


def get_overview(org_id, days=30):
    """
    Pega metricas gerais do GA4: sessions, users, pageviews, bounce rate, etc.
    """
    config = _get_config(org_id)
    if not config:
        return {'ok': False, 'error': 'GA4 nao configurado'}

    body = {
        'dateRanges': [{'startDate': f'{days}daysAgo', 'endDate': 'today'}],
        'metrics': [
            {'name': 'sessions'},
            {'name': 'totalUsers'},
            {'name': 'newUsers'},
            {'name': 'screenPageViews'},
            {'name': 'bounceRate'},
            {'name': 'averageSessionDuration'},
            {'name': 'conversions'},
            {'name': 'totalRevenue'},
        ],
    }
    result = _ga4_api(config['property_id'], config['access_token'], 'runReport', body)
    if result.get('error'):
        return {'ok': False, 'error': result}

    rows = result.get('rows', [])
    if not rows:
        return {'ok': True, 'data': {}, 'empty': True}

    values = rows[0].get('metricValues', [])
    data = {
        'sessions': int(values[0].get('value', 0)) if len(values) > 0 else 0,
        'users': int(values[1].get('value', 0)) if len(values) > 1 else 0,
        'new_users': int(values[2].get('value', 0)) if len(values) > 2 else 0,
        'pageviews': int(values[3].get('value', 0)) if len(values) > 3 else 0,
        'bounce_rate': round(float(values[4].get('value', 0)) * 100, 1) if len(values) > 4 else 0,
        'avg_session_sec': round(float(values[5].get('value', 0)), 1) if len(values) > 5 else 0,
        'conversions': int(values[6].get('value', 0)) if len(values) > 6 else 0,
        'revenue': round(float(values[7].get('value', 0)), 2) if len(values) > 7 else 0,
        'period_days': days,
    }
    return {'ok': True, 'data': data}


def get_traffic_sources(org_id, days=30, limit=20):
    """Retorna trafego por source/medium."""
    config = _get_config(org_id)
    if not config:
        return {'ok': False, 'error': 'GA4 nao configurado'}

    body = {
        'dateRanges': [{'startDate': f'{days}daysAgo', 'endDate': 'today'}],
        'dimensions': [
            {'name': 'sessionSource'},
            {'name': 'sessionMedium'},
        ],
        'metrics': [
            {'name': 'sessions'},
            {'name': 'totalUsers'},
            {'name': 'conversions'},
            {'name': 'totalRevenue'},
        ],
        'limit': limit,
        'orderBys': [{'metric': {'metricName': 'sessions'}, 'desc': True}],
    }
    result = _ga4_api(config['property_id'], config['access_token'], 'runReport', body)
    if result.get('error'):
        return {'ok': False, 'error': result}

    sources = []
    for row in result.get('rows', []):
        dims = row.get('dimensionValues', [])
        vals = row.get('metricValues', [])
        sources.append({
            'source': dims[0].get('value', '(direct)') if dims else '(direct)',
            'medium': dims[1].get('value', '(none)') if len(dims) > 1 else '(none)',
            'sessions': int(vals[0].get('value', 0)) if vals else 0,
            'users': int(vals[1].get('value', 0)) if len(vals) > 1 else 0,
            'conversions': int(vals[2].get('value', 0)) if len(vals) > 2 else 0,
            'revenue': round(float(vals[3].get('value', 0)), 2) if len(vals) > 3 else 0,
        })
    return {'ok': True, 'sources': sources}


def get_top_pages(org_id, days=30, limit=20):
    """Retorna paginas mais visitadas."""
    config = _get_config(org_id)
    if not config:
        return {'ok': False, 'error': 'GA4 nao configurado'}

    body = {
        'dateRanges': [{'startDate': f'{days}daysAgo', 'endDate': 'today'}],
        'dimensions': [{'name': 'pagePath'}],
        'metrics': [
            {'name': 'screenPageViews'},
            {'name': 'averageSessionDuration'},
            {'name': 'bounceRate'},
        ],
        'limit': limit,
        'orderBys': [{'metric': {'metricName': 'screenPageViews'}, 'desc': True}],
    }
    result = _ga4_api(config['property_id'], config['access_token'], 'runReport', body)
    if result.get('error'):
        return {'ok': False, 'error': result}

    pages = []
    for row in result.get('rows', []):
        dims = row.get('dimensionValues', [])
        vals = row.get('metricValues', [])
        pages.append({
            'path': dims[0].get('value', '/') if dims else '/',
            'pageviews': int(vals[0].get('value', 0)) if vals else 0,
            'avg_time_sec': round(float(vals[1].get('value', 0)), 1) if len(vals) > 1 else 0,
            'bounce_rate': round(float(vals[2].get('value', 0)) * 100, 1) if len(vals) > 2 else 0,
        })
    return {'ok': True, 'pages': pages}


def _get_config(org_id):
    """Busca config GA4 do org (property_id + access_token)."""
    db = get_db()
    row = db.execute(
        "SELECT * FROM ga4_config WHERE org_id=? AND status='connected'",
        (org_id,)
    ).fetchone()
    if row:
        db.close()
        return dict(row)
    # Fallback: buscar do oauth_manager (integrations table)
    row2 = db.execute(
        "SELECT access_token, config_json FROM api_integrations WHERE org_id=? AND platform='google_analytics' AND status='connected'",
        (org_id,)
    ).fetchone()
    db.close()
    if row2:
        config = json.loads(row2['config_json'] or '{}')
        return {
            'property_id': config.get('property_id', ''),
            'access_token': row2['access_token'],
        }
    return None


def save_config(org_id, property_id, access_token=None, refresh_token=None):
    """Salva config GA4."""
    db = get_db()
    db.execute(
        '''INSERT OR REPLACE INTO ga4_config
           (org_id, property_id, access_token, refresh_token, status)
           VALUES (?, ?, ?, ?, 'connected')''',
        (org_id, property_id, access_token, refresh_token)
    )
    db.commit()
    db.close()
