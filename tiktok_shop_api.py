# tiktok_shop_api.py — Integracao com TikTok Shop Open Platform
# Docs oficiais: https://partner.tiktokshop.com/docv2/
#
# Fluxo OAuth:
#   1. App cadastrado em https://partner.tiktokshop.com
#   2. Seller clica em "Conectar TikTok Shop" no Sellvance
#   3. Redirect para partner.tiktokshop.com com app_key + state
#   4. Seller autoriza e volta pro nosso /oauth/tiktok_shop/callback com auth_code
#   5. Trocamos auth_code por access_token + refresh_token
#   6. Access token expira em 7 dias — refresh automatico
#
# APIs principais:
#   - /api/orders/search     — listar pedidos
#   - /api/products/search   — listar produtos
#   - /api/logistics/ship    — atualizar tracking
#   - /api/product/price/update — atualizar preco (usado pelo pricing_ai)

import os
import hmac
import hashlib
import time
import json
import urllib.request
import urllib.parse
from database import get_db


# Configuracao (puxa de env vars)
TT_APP_KEY = os.environ.get('TIKTOK_SHOP_APP_KEY', '')
TT_APP_SECRET = os.environ.get('TIKTOK_SHOP_APP_SECRET', '')
TT_API_BASE = 'https://open-api.tiktokglobalshop.com'
TT_AUTH_BASE = 'https://auth.tiktok-shops.com'
TT_REDIRECT_URI = os.environ.get(
    'TIKTOK_SHOP_REDIRECT_URI',
    'https://www.sellvance.com.br/oauth/tiktok_shop/callback'
)


def ensure_tables():
    """Bootstrap das tabelas de TikTok Shop."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS tiktok_shop_credentials (
            org_id          INTEGER PRIMARY KEY,
            shop_id         TEXT,
            shop_name       TEXT,
            shop_cipher     TEXT,
            access_token    TEXT,
            refresh_token   TEXT,
            access_expires  TEXT,
            refresh_expires TEXT,
            open_id         TEXT,
            seller_name     TEXT,
            region          TEXT DEFAULT 'BR',
            status          TEXT DEFAULT 'disconnected',
            last_sync       TEXT,
            last_error      TEXT,
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS tiktok_shop_sync_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            endpoint        TEXT,
            status          TEXT,
            items_synced    INTEGER DEFAULT 0,
            error           TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );
    ''')
    db.commit()
    db.close()


def _sign_request(params, path, body=''):
    """
    Assinatura TikTok Shop Open API:
      1. Ordena params (exceto 'sign' e 'access_token')
      2. Concatena: app_secret + path + (k+v)... + body + app_secret
      3. SHA256 em HMAC do resultado
    """
    filtered = {k: v for k, v in params.items() if k not in ('sign', 'access_token')}
    sorted_keys = sorted(filtered.keys())

    sign_str = TT_APP_SECRET + path
    for k in sorted_keys:
        sign_str += f'{k}{filtered[k]}'
    sign_str += body
    sign_str += TT_APP_SECRET

    signature = hmac.new(
        TT_APP_SECRET.encode('utf-8'),
        sign_str.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    return signature


def get_auth_url(state):
    """Retorna URL de autorizacao para o seller abrir."""
    params = {
        'app_key': TT_APP_KEY,
        'state': state,
    }
    return f'{TT_AUTH_BASE}/oauth/authorize?{urllib.parse.urlencode(params)}'


def exchange_auth_code(auth_code):
    """
    Troca auth_code por access_token (primeiro passo apos autorizacao).
    Endpoint nao assinado — usa app_key + app_secret + auth_code.
    """
    url = f'{TT_AUTH_BASE}/api/v2/token/get'
    params = {
        'app_key': TT_APP_KEY,
        'app_secret': TT_APP_SECRET,
        'auth_code': auth_code,
        'grant_type': 'authorized_code',
    }
    full_url = f'{url}?{urllib.parse.urlencode(params)}'
    try:
        req = urllib.request.Request(full_url, method='GET')
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        if data.get('code') == 0:
            return {'ok': True, 'data': data.get('data', {})}
        return {'ok': False, 'error': data.get('message', 'Unknown error'), 'raw': data}
    except Exception as e:
        return {'ok': False, 'error': str(e)[:200]}


def refresh_access_token(org_id):
    """Usa refresh_token para gerar novo access_token (7 dias)."""
    db = get_db()
    row = db.execute(
        'SELECT refresh_token FROM tiktok_shop_credentials WHERE org_id=?',
        (org_id,)
    ).fetchone()
    db.close()
    if not row or not row['refresh_token']:
        return {'ok': False, 'error': 'sem refresh_token'}

    url = f'{TT_AUTH_BASE}/api/v2/token/refresh'
    params = {
        'app_key': TT_APP_KEY,
        'app_secret': TT_APP_SECRET,
        'refresh_token': row['refresh_token'],
        'grant_type': 'refresh_token',
    }
    full_url = f'{url}?{urllib.parse.urlencode(params)}'
    try:
        req = urllib.request.Request(full_url, method='GET')
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        if data.get('code') == 0:
            d = data.get('data', {})
            db = get_db()
            db.execute(
                '''UPDATE tiktok_shop_credentials
                   SET access_token=?, refresh_token=?,
                       access_expires=datetime('now', '+7 days'),
                       updated_at=datetime('now')
                   WHERE org_id=?''',
                (d.get('access_token'), d.get('refresh_token'), org_id)
            )
            db.commit()
            db.close()
            return {'ok': True}
        return {'ok': False, 'error': data.get('message'), 'raw': data}
    except Exception as e:
        return {'ok': False, 'error': str(e)[:200]}


def save_credentials(org_id, token_data):
    """Salva credenciais retornadas do OAuth."""
    db = get_db()
    db.execute(
        '''INSERT OR REPLACE INTO tiktok_shop_credentials
           (org_id, access_token, refresh_token, open_id,
            seller_name, status, access_expires, refresh_expires, updated_at)
           VALUES (?, ?, ?, ?, ?, 'connected',
                   datetime('now', '+7 days'),
                   datetime('now', '+30 days'),
                   datetime('now'))''',
        (org_id,
         token_data.get('access_token'),
         token_data.get('refresh_token'),
         token_data.get('open_id'),
         token_data.get('seller_name', ''))
    )
    db.commit()
    db.close()
    return True


def get_credentials(org_id):
    db = get_db()
    row = db.execute(
        'SELECT * FROM tiktok_shop_credentials WHERE org_id=?',
        (org_id,)
    ).fetchone()
    db.close()
    return dict(row) if row else None


def _api_call(org_id, path, method='GET', params=None, body=None):
    """
    Chamada generica com assinatura e refresh automatico.
    """
    creds = get_credentials(org_id)
    if not creds or creds.get('status') != 'connected':
        return {'ok': False, 'error': 'TikTok Shop nao conectado'}

    # Refresh automatico se proximo do vencimento
    # (checagem simples: se access_expires <= now + 1 dia)
    try:
        db = get_db()
        row = db.execute(
            '''SELECT CAST(julianday(access_expires) - julianday('now') AS REAL) as days_left
               FROM tiktok_shop_credentials WHERE org_id=?''',
            (org_id,)
        ).fetchone()
        db.close()
        if row and row['days_left'] is not None and row['days_left'] < 1:
            refresh_access_token(org_id)
            creds = get_credentials(org_id)
    except Exception:
        pass

    params = params or {}
    params['app_key'] = TT_APP_KEY
    params['timestamp'] = str(int(time.time()))
    params['shop_id'] = creds.get('shop_id', '')
    params['version'] = '202309'

    body_str = json.dumps(body) if body else ''
    params['sign'] = _sign_request(params, path, body_str)
    params['access_token'] = creds['access_token']

    url = f'{TT_API_BASE}{path}?{urllib.parse.urlencode(params)}'
    try:
        if body:
            req = urllib.request.Request(
                url,
                data=body_str.encode('utf-8'),
                headers={'Content-Type': 'application/json'},
                method=method
            )
        else:
            req = urllib.request.Request(url, method=method)
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
        return {'ok': data.get('code') == 0, 'data': data.get('data'), 'raw': data}
    except urllib.error.HTTPError as e:
        err = e.read().decode('utf-8', errors='replace')[:500]
        return {'ok': False, 'error': f'HTTP {e.code}: {err[:200]}'}
    except Exception as e:
        return {'ok': False, 'error': str(e)[:200]}


def sync_products(org_id, page_size=50):
    """Sincroniza lista de produtos do TikTok Shop."""
    result = _api_call(
        org_id,
        '/product/202309/products/search',
        method='POST',
        body={'page_size': page_size, 'page_number': 1}
    )
    if not result['ok']:
        _log_sync(org_id, '/product/search', 'error', error=result.get('error'))
        return result

    items = (result.get('data') or {}).get('products', [])
    synced = 0
    db = get_db()
    for p in items:
        sku = p.get('id', '')
        name = p.get('title', '')
        # Simplificado: pega primeiro SKU/preco
        skus_list = p.get('skus', [])
        sale_price = 0
        if skus_list:
            sale_price = float(skus_list[0].get('price', {}).get('sale_price', 0) or 0)
        try:
            db.execute(
                '''INSERT OR REPLACE INTO stock_items
                   (org_id, sku, name, marketplace, sale_price, last_updated)
                   VALUES (?, ?, ?, 'tiktok_shop', ?, datetime('now'))''',
                (org_id, sku, name, sale_price)
            )
            synced += 1
        except Exception:
            continue
    db.commit()
    db.close()
    _log_sync(org_id, '/product/search', 'ok', items_synced=synced)
    return {'ok': True, 'synced': synced}


def sync_orders(org_id, days_back=7):
    """Sincroniza pedidos recentes."""
    start_ts = int(time.time()) - (days_back * 86400)
    result = _api_call(
        org_id,
        '/order/202309/orders/search',
        method='POST',
        body={
            'page_size': 100,
            'page_number': 1,
            'create_time_ge': start_ts,
            'create_time_lt': int(time.time()),
        }
    )
    if not result['ok']:
        _log_sync(org_id, '/order/search', 'error', error=result.get('error'))
        return result

    orders_raw = (result.get('data') or {}).get('orders', [])
    synced = 0
    db = get_db()
    for o in orders_raw:
        order_id = o.get('id', '')
        total = float(o.get('payment', {}).get('total_amount', 0) or 0)
        try:
            db.execute(
                '''INSERT OR IGNORE INTO orders
                   (org_id, channel, external_order_id, revenue, status, created_at)
                   VALUES (?, 'tiktok_shop', ?, ?, ?, datetime('now'))''',
                (org_id, order_id, total, o.get('status', 'unknown'))
            )
            synced += 1
        except Exception:
            continue
    db.commit()
    db.close()
    _log_sync(org_id, '/order/search', 'ok', items_synced=synced)
    return {'ok': True, 'synced': synced}


def update_product_price(org_id, product_id, sku_id, new_price):
    """
    Atualiza preco de um SKU no TikTok Shop.
    Usado pelo pricing_ai quando auto_apply=1.
    """
    return _api_call(
        org_id,
        f'/product/202309/products/{product_id}/prices/update',
        method='POST',
        body={
            'skus': [{'id': sku_id, 'price': {'amount': str(new_price), 'currency': 'BRL'}}]
        }
    )


def _log_sync(org_id, endpoint, status, items_synced=0, error=None):
    """Registra um evento de sync."""
    try:
        db = get_db()
        db.execute(
            '''INSERT INTO tiktok_shop_sync_log
               (org_id, endpoint, status, items_synced, error)
               VALUES (?, ?, ?, ?, ?)''',
            (org_id, endpoint, status, items_synced, error)
        )
        if status == 'ok':
            db.execute(
                "UPDATE tiktok_shop_credentials SET last_sync=datetime('now') WHERE org_id=?",
                (org_id,)
            )
        db.commit()
        db.close()
    except Exception:
        pass


def disconnect(org_id):
    """Remove credenciais (usuario clicou em 'desconectar')."""
    db = get_db()
    db.execute(
        "UPDATE tiktok_shop_credentials SET status='disconnected', access_token=NULL, refresh_token=NULL WHERE org_id=?",
        (org_id,)
    )
    db.commit()
    db.close()
    return True


def is_configured():
    """Checa se TT_APP_KEY e TT_APP_SECRET estao configurados."""
    return bool(TT_APP_KEY and TT_APP_SECRET)
