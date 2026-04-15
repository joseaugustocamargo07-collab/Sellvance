# Shopee Open Platform API v2 — Integration Module
# OAuth flow + Products + Orders sync
import hmac
import hashlib
import time
import json
import urllib.request
import urllib.error
import urllib.parse
import os

# ── Credentials (runtime lookup — refletem mudancas de env sem reimport) ──
def _get_partner_id():
    return int(os.environ.get('SHOPEE_PARTNER_ID', '1231483'))

def _get_partner_key():
    # strip() remove espacos/quebras de linha acidentais ao colar no Railway
    return (os.environ.get('SHOPEE_PARTNER_KEY', 'shpk4b654753636764416547594961756c6f7749457254747468434a51504c53') or '').strip()

def _get_api_host():
    env = (os.environ.get('SHOPEE_ENV', 'sandbox') or '').strip().lower()
    if env == 'production':
        return 'https://partner.shopeemobile.com'
    return 'https://partner.test-stable.shopeemobile.com'

def _get_redirect_url():
    return os.environ.get('SHOPEE_REDIRECT_URL', 'https://www.sellvance.com.br/api/shopee/callback')

# Backwards compat — codigo antigo que le PARTNER_ID/PARTNER_KEY/API_HOST continua funcionando
# mas o valor e recalculado a cada acesso via property-like pattern nao e possivel em modulo,
# entao mantemos os lookups antigos como snapshot inicial (pode ficar desatualizado)
PARTNER_ID = _get_partner_id()
PARTNER_KEY = _get_partner_key()
API_HOST = _get_api_host()
REDIRECT_URL = _get_redirect_url()
_SHOPEE_ENV = (os.environ.get('SHOPEE_ENV', 'sandbox') or '').strip().lower()


# ── Signature helpers ────────────────────────────────────────────────────────

def _sign(path, timestamp, access_token='', shop_id=0):
    """Generate HMAC-SHA256 signature for Shopee API v2."""
    base_string = f'{PARTNER_ID}{path}{timestamp}'
    if access_token:
        base_string += f'{access_token}{shop_id}'
    return hmac.new(PARTNER_KEY.encode(), base_string.encode(), hashlib.sha256).hexdigest()


def _make_url(path, **params):
    """Build full API URL with authentication params."""
    ts = int(time.time())
    access_token = params.pop('access_token', '')
    shop_id = params.pop('shop_id', 0)
    sign = _sign(path, ts, access_token, shop_id)

    query = {
        'partner_id': PARTNER_ID,
        'timestamp': ts,
        'sign': sign,
    }
    if access_token:
        query['access_token'] = access_token
        query['shop_id'] = shop_id
    query.update(params)

    return f'{API_HOST}{path}?{urllib.parse.urlencode(query)}'


def _api_get(path, access_token='', shop_id=0, **params):
    """Make authenticated GET request to Shopee API."""
    url = _make_url(path, access_token=access_token, shop_id=shop_id, **params)
    req = urllib.request.Request(url, headers={
        'Content-Type': 'application/json',
        'User-Agent': 'Sellvance/1.0',
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ''
        return {'error': e.code, 'message': body}
    except Exception as e:
        return {'error': str(e)}


def _api_post(path, data, access_token='', shop_id=0):
    """Make authenticated POST request to Shopee API."""
    url = _make_url(path, access_token=access_token, shop_id=shop_id)
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method='POST', headers={
        'Content-Type': 'application/json',
        'User-Agent': 'Sellvance/1.0',
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ''
        return {'error': e.code, 'message': body}
    except Exception as e:
        return {'error': str(e)}


# ── OAuth Flow ───────────────────────────────────────────────────────────────

def get_auth_url(redirect_url=None):
    """Generate the Shopee OAuth authorization URL.
    The seller clicks this to authorize their shop."""
    path = '/api/v2/shop/auth_partner'
    ts = int(time.time())
    redirect = redirect_url or REDIRECT_URL
    base_string = f'{PARTNER_ID}{path}{ts}'
    sign = hmac.new(PARTNER_KEY.encode(), base_string.encode(), hashlib.sha256).hexdigest()

    params = {
        'partner_id': PARTNER_ID,
        'timestamp': ts,
        'sign': sign,
        'redirect': redirect,
    }
    return f'{API_HOST}{path}?{urllib.parse.urlencode(params)}'


def get_access_token(code, shop_id):
    """Exchange authorization code for access token."""
    # Runtime lookup — garante que pega o valor atual das env vars
    partner_id = _get_partner_id()
    partner_key = _get_partner_key()
    api_host = _get_api_host()

    path = '/api/v2/auth/token/get'
    ts = int(time.time())
    base_string = f'{partner_id}{path}{ts}'
    sign = hmac.new(partner_key.encode(), base_string.encode(), hashlib.sha256).hexdigest()

    url = f'{api_host}{path}?partner_id={partner_id}&timestamp={ts}&sign={sign}'
    data = {
        'code': code,
        'shop_id': int(shop_id),
        'partner_id': partner_id,
    }
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method='POST', headers={
        'Content-Type': 'application/json',
        'User-Agent': 'Sellvance/1.0',
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode() if e.fp else ''
        return {'error': e.code, 'message': err_body}
    except Exception as e:
        return {'error': str(e)}


def refresh_access_token(refresh_token, shop_id):
    """Refresh an expired access token."""
    path = '/api/v2/auth/access_token/get'
    ts = int(time.time())
    base_string = f'{PARTNER_ID}{path}{ts}'
    sign = hmac.new(PARTNER_KEY.encode(), base_string.encode(), hashlib.sha256).hexdigest()

    url = f'{API_HOST}{path}?partner_id={PARTNER_ID}&timestamp={ts}&sign={sign}'
    data = {
        'refresh_token': refresh_token,
        'shop_id': int(shop_id),
        'partner_id': PARTNER_ID,
    }
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method='POST', headers={
        'Content-Type': 'application/json',
        'User-Agent': 'Sellvance/1.0',
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode() if e.fp else ''
        return {'error': e.code, 'message': err_body}
    except Exception as e:
        return {'error': str(e)}


# ── Shop Info ────────────────────────────────────────────────────────────────

def get_shop_info(access_token, shop_id):
    """Get basic shop information."""
    return _api_get('/api/v2/shop/get_shop_info',
                    access_token=access_token, shop_id=shop_id)


# ── Products ─────────────────────────────────────────────────────────────────

def get_item_list(access_token, shop_id, offset=0, page_size=50):
    """Get list of product IDs from the shop."""
    return _api_get('/api/v2/product/get_item_list',
                    access_token=access_token, shop_id=shop_id,
                    offset=str(offset), page_size=str(page_size),
                    item_status='NORMAL')


def get_item_base_info(access_token, shop_id, item_ids):
    """Get detailed info for specific items. item_ids is a list of ints."""
    ids_str = ','.join(str(i) for i in item_ids)
    return _api_get('/api/v2/product/get_item_base_info',
                    access_token=access_token, shop_id=shop_id,
                    item_id_list=ids_str)


# ── Orders ───────────────────────────────────────────────────────────────────

def get_order_list(access_token, shop_id, time_from, time_to,
                   order_status='COMPLETED', page_size=50, cursor=''):
    """Get list of orders within a time range."""
    params = {
        'time_range_field': 'create_time',
        'time_from': str(int(time_from)),
        'time_to': str(int(time_to)),
        'page_size': str(page_size),
        'order_status': order_status,
    }
    if cursor:
        params['cursor'] = cursor
    return _api_get('/api/v2/order/get_order_list',
                    access_token=access_token, shop_id=shop_id,
                    **params)


def get_order_detail(access_token, shop_id, order_sn_list):
    """Get detailed order info. order_sn_list is a list of order SN strings."""
    sns = ','.join(order_sn_list)
    return _api_get('/api/v2/order/get_order_detail',
                    access_token=access_token, shop_id=shop_id,
                    order_sn_list=sns,
                    response_optional_fields='item_list,buyer_username,pay_time,total_amount')


# ── Sync All ─────────────────────────────────────────────────────────────────

def sync_all(org_id, db=None):
    """Full sync: products + orders from Shopee.
    Reads credentials from api_integrations table.
    Returns dict with counts."""
    if db is None:
        from database import get_db
        db = get_db()

    row = db.execute(
        "SELECT * FROM api_integrations WHERE org_id=? AND platform='shopee' AND status='connected'",
        (org_id,)).fetchone()
    if not row:
        return {'error': 'Shopee não conectada', 'products': 0, 'orders': 0}

    creds = json.loads(row['credentials_json'] or '{}')
    access_token = creds.get('access_token', '')
    refresh_token = creds.get('refresh_token', '')
    shop_id = int(creds.get('shop_id', 0))
    token_expire = creds.get('expire_in_ts', 0)

    if not access_token or not shop_id:
        return {'error': 'Token de acesso não encontrado', 'products': 0, 'orders': 0}

    # Refresh token if expired
    now = int(time.time())
    if token_expire and now >= token_expire - 300:  # refresh 5 min before expiry
        result = refresh_access_token(refresh_token, shop_id)
        if 'error' in result and not result.get('access_token'):
            return {'error': f'Falha ao renovar token: {result}', 'products': 0, 'orders': 0}
        access_token = result.get('access_token', access_token)
        new_refresh = result.get('refresh_token', refresh_token)
        expire_in = result.get('expire_in', 14400)
        creds['access_token'] = access_token
        creds['refresh_token'] = new_refresh
        creds['expire_in_ts'] = now + expire_in
        db.execute("UPDATE api_integrations SET credentials_json=? WHERE id=?",
                   (json.dumps(creds), row['id']))
        db.commit()

    # ── Sync Products ──
    product_count = 0
    try:
        items_resp = get_item_list(access_token, shop_id)
        items = items_resp.get('response', {}).get('item', [])
        if items:
            item_ids = [it['item_id'] for it in items]
            # Fetch in batches of 50
            for i in range(0, len(item_ids), 50):
                batch = item_ids[i:i+50]
                detail = get_item_base_info(access_token, shop_id, batch)
                item_list = detail.get('response', {}).get('item_list', [])
                for item in item_list:
                    ext_id = str(item.get('item_id', ''))
                    title = item.get('item_name', '')
                    price = 0
                    models = item.get('price_info', [])
                    if models:
                        price = models[0].get('current_price', 0) if isinstance(models, list) else models.get('current_price', 0)
                    # Try getting price from another field
                    if not price:
                        price_info = item.get('price_info', {})
                        if isinstance(price_info, dict):
                            price = price_info.get('current_price', price_info.get('original_price', 0))

                    stock = 0
                    stock_info = item.get('stock_info_v2', {})
                    if isinstance(stock_info, dict):
                        stock = stock_info.get('current_stock', stock_info.get('normal_stock', 0))

                    status = 'active' if item.get('item_status') == 'NORMAL' else 'inactive'
                    image = ''
                    imgs = item.get('image', {})
                    if isinstance(imgs, dict) and imgs.get('image_url_list'):
                        image = imgs['image_url_list'][0]

                    category = item.get('category_id', '')

                    existing = db.execute(
                        "SELECT id FROM mp_products WHERE org_id=? AND platform='shopee' AND external_id=?",
                        (org_id, ext_id)).fetchone()
                    if existing:
                        db.execute("""UPDATE mp_products SET title=?, price=?, stock=?, status=?,
                                     image_url=?, category=?, last_synced=datetime('now')
                                     WHERE id=?""",
                                   (title, price, stock, status, image, str(category), existing['id']))
                    else:
                        db.execute("""INSERT INTO mp_products
                                     (org_id, platform, external_id, title, price, stock, status, image_url, category, last_synced)
                                     VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))""",
                                   (org_id, 'shopee', ext_id, title, price, stock, status, image, str(category)))
                    product_count += 1
    except Exception as e:
        print(f'[Shopee] Product sync error: {e}')

    # ── Sync Orders (last 30 days) ──
    order_count = 0
    try:
        time_to = int(time.time())
        time_from = time_to - (30 * 86400)  # 30 days
        all_order_sns = []

        # Fetch all order statuses
        for status in ['COMPLETED', 'SHIPPED', 'READY_TO_SHIP', 'IN_CANCEL', 'CANCELLED']:
            cursor = ''
            while True:
                resp = get_order_list(access_token, shop_id, time_from, time_to,
                                     order_status=status, cursor=cursor)
                response = resp.get('response', {})
                orders = response.get('order_list', [])
                for o in orders:
                    all_order_sns.append(o['order_sn'])
                if not response.get('more', False):
                    break
                cursor = response.get('next_cursor', '')

        # Fetch details in batches of 50
        for i in range(0, len(all_order_sns), 50):
            batch = all_order_sns[i:i+50]
            detail_resp = get_order_detail(access_token, shop_id, batch)
            order_list = detail_resp.get('response', {}).get('order_list', [])
            for order in order_list:
                order_sn = order.get('order_sn', '')
                total = float(order.get('total_amount', 0))
                order_status = order.get('order_status', '')
                create_time = order.get('create_time', 0)
                pay_time = order.get('pay_time', 0)
                buyer = order.get('buyer_username', '')

                # Map Shopee status to our status
                status_map = {
                    'COMPLETED': 'delivered',
                    'SHIPPED': 'shipped',
                    'READY_TO_SHIP': 'processing',
                    'IN_CANCEL': 'cancelled',
                    'CANCELLED': 'cancelled',
                    'UNPAID': 'pending',
                }
                mapped_status = status_map.get(order_status, 'processing')

                # Items
                items = order.get('item_list', [])
                item_count = sum(it.get('model_quantity_purchased', 1) for it in items)

                # Convert timestamps
                from datetime import datetime
                order_date = datetime.fromtimestamp(create_time).strftime('%Y-%m-%d %H:%M:%S') if create_time else ''

                existing_order = db.execute(
                    "SELECT id FROM orders WHERE org_id=? AND marketplace='shopee' AND external_id=?",
                    (org_id, order_sn)).fetchone()
                if existing_order:
                    db.execute("""UPDATE orders SET total_amount=?, status=?, items_count=?
                                 WHERE id=?""",
                               (total, mapped_status, item_count, existing_order['id']))
                else:
                    db.execute("""INSERT INTO orders
                                 (org_id, marketplace, external_id, total_amount, status, items_count,
                                  customer_name, order_date)
                                 VALUES (?,?,?,?,?,?,?,?)""",
                               (org_id, 'shopee', order_sn, total, mapped_status, item_count,
                                buyer, order_date))
                order_count += 1
    except Exception as e:
        print(f'[Shopee] Order sync error: {e}')

    # Update last sync
    db.execute("UPDATE api_integrations SET last_sync=datetime('now') WHERE org_id=? AND platform='shopee'",
               (org_id,))
    db.commit()

    return {'products': product_count, 'orders': order_count}
