"""
Sellvance — Amazon SP-API Sync
Syncs orders, products and account health from Amazon Seller Central.

Authentication flow:
  1. Exchange refresh_token for LWA access_token  (per seller)
  2. Sign each request with AWS Signature v4       (platform-wide IAM creds)

Railway env vars needed (set once, platform-wide):
  AMAZON_AWS_ACCESS_KEY  — IAM Access Key ID
  AMAZON_AWS_SECRET_KEY  — IAM Secret Access Key

Per-seller credentials saved in api_integrations.config_json:
  seller_id       — Seller ID  (e.g. A1B2C3D4E5F6G7)
  marketplace_id  — Marketplace ID  (A2Q3Y263D00KWC = Brazil)
  client_id       — LWA Client ID   (amzn1.application-oa2-client.xxx)
  client_secret   — LWA Client Secret
  refresh_token   — LWA Refresh Token  (Atexxxx...)
"""

import json, hashlib, hmac, datetime, urllib.request, urllib.parse, urllib.error, os
from database import get_db
from sync_base import AuthError

# ── SP-API region routing ─────────────────────────────────────────────────────
_ENDPOINT = {
    'A2Q3Y263D00KWC': 'sellingpartnerapi-na.amazon.com',  # Brazil
    'ATVPDKIKX0DER':  'sellingpartnerapi-na.amazon.com',  # US
    'A2EUQ1WTGCTBG2': 'sellingpartnerapi-na.amazon.com',  # Canada
    'A1AM78C64UM0Y8': 'sellingpartnerapi-na.amazon.com',  # Mexico
    'A1RKKUPIHCS9HS': 'sellingpartnerapi-eu.amazon.com',  # Spain
    'A13V1IB3VIYZZH': 'sellingpartnerapi-eu.amazon.com',  # France
    'A1F83G8C2ARO7P': 'sellingpartnerapi-eu.amazon.com',  # UK
    'A1PA6795UKMFR9': 'sellingpartnerapi-eu.amazon.com',  # Germany
    'APJ6JRA9NG5V4':  'sellingpartnerapi-eu.amazon.com',  # Italy
    'A1VC38T7YXB528': 'sellingpartnerapi-fe.amazon.com',  # Japan
}
_DEFAULT_ENDPOINT = 'sellingpartnerapi-na.amazon.com'
LWA_URL = 'https://api.amazon.com/auth/o2/token'


# ── LWA token ─────────────────────────────────────────────────────────────────

def _get_lwa_token(client_id, client_secret, refresh_token):
    """Exchange refresh_token → LWA access_token."""
    body = urllib.parse.urlencode({
        'grant_type':    'refresh_token',
        'client_id':     client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
    }).encode('utf-8')
    req = urllib.request.Request(
        LWA_URL, data=body,
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        method='POST')
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        token = data.get('access_token', '')
        if not token:
            raise AuthError(f"LWA returned no access_token: {data}")
        return token, data.get('expires_in', 3600)
    except urllib.error.HTTPError as e:
        body_err = e.read().decode()[:300]
        print(f"[amazon] LWA error {e.code}: {body_err}")
        if e.code in (400, 401):
            raise AuthError(f"LWA auth failed ({e.code}): {body_err}")
        raise


# ── AWS Signature v4 (pure stdlib, no boto3) ──────────────────────────────────

def _sha256hex(data):
    if isinstance(data, str):
        data = data.encode('utf-8')
    return hashlib.sha256(data).hexdigest()

def _hmac256(key, msg):
    if isinstance(msg, str):
        msg = msg.encode('utf-8')
    return hmac.new(key, msg, hashlib.sha256).digest()

def _signing_key(secret, date_str, region, service):
    kdate    = _hmac256(('AWS4' + secret).encode('utf-8'), date_str)
    kregion  = _hmac256(kdate,   region)
    kservice = _hmac256(kregion, service)
    return     _hmac256(kservice, 'aws4_request')

def _sigv4_headers(method, url, extra_headers, body_bytes,
                   aws_key, aws_secret, region='us-east-1', service='execute-api'):
    """Return headers dict with AWS4-HMAC-SHA256 Authorization added."""
    parsed   = urllib.parse.urlparse(url)
    now      = datetime.datetime.utcnow()
    amz_date = now.strftime('%Y%m%dT%H%M%SZ')
    date_str = now.strftime('%Y%m%d')
    host     = parsed.netloc
    path     = parsed.path or '/'
    query_str = parsed.query or ''

    hdrs = dict(extra_headers)
    hdrs['x-amz-date'] = amz_date
    hdrs['host']        = host

    sorted_keys    = sorted(hdrs.keys(), key=str.lower)
    signed_headers = ';'.join(k.lower() for k in sorted_keys)
    canonical_hdrs = ''.join(f"{k.lower()}:{hdrs[k].strip()}\n" for k in sorted_keys)

    payload_hash  = _sha256hex(body_bytes)
    canonical_req = '\n'.join([
        method.upper(), path, query_str,
        canonical_hdrs, signed_headers, payload_hash])

    cred_scope  = f"{date_str}/{region}/{service}/aws4_request"
    string_sign = '\n'.join([
        'AWS4-HMAC-SHA256', amz_date, cred_scope, _sha256hex(canonical_req)])

    sig = hmac.new(
        _signing_key(aws_secret, date_str, region, service),
        string_sign.encode('utf-8'), hashlib.sha256).hexdigest()

    hdrs['Authorization'] = (
        f"AWS4-HMAC-SHA256 Credential={aws_key}/{cred_scope}, "
        f"SignedHeaders={signed_headers}, Signature={sig}")
    hdrs.pop('host', None)
    return hdrs


# ── SP-API HTTP helper ─────────────────────────────────────────────────────────

def _sp_get(endpoint, path, access_token, params=None,
            aws_key='', aws_secret='', region='us-east-1'):
    """Signed GET to SP-API. Returns parsed JSON or {}."""
    qs  = ('?' + urllib.parse.urlencode(params)) if params else ''
    url = f"https://{endpoint}{path}{qs}"

    base_hdrs = {
        'x-amz-access-token': access_token,
        'Content-Type':       'application/json',
    }
    if aws_key and aws_secret:
        base_hdrs = _sigv4_headers('GET', url, base_hdrs, b'',
                                   aws_key, aws_secret, region)

    req = urllib.request.Request(url, headers=base_hdrs, method='GET')
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_err = e.read().decode()[:400]
        print(f"[amazon SP-API] {e.code} {path}: {body_err}")
        if e.code in (401, 403):
            raise AuthError(f"SP-API {e.code}: {body_err[:100]}")
        if e.code == 429:
            import time; time.sleep(3)
        return {}
    except Exception as exc:
        print(f"[amazon SP-API] {path}: {exc}")
        return {}


# ── Credential loader ──────────────────────────────────────────────────────────

def _load_creds(org_id):
    from oauth_manager import get_integration
    integ = get_integration(org_id, 'amazon')
    if not integ or integ.get('status') != 'connected':
        return None
    cfg   = integ.get('config', {})
    mp_id = cfg.get('marketplace_id', 'A2Q3Y263D00KWC')
    return {
        'seller_id':      cfg.get('seller_id', ''),
        'marketplace_id': mp_id,
        'client_id':      cfg.get('client_id', ''),
        'client_secret':  cfg.get('client_secret', ''),
        'refresh_token':  cfg.get('refresh_token', ''),
        'aws_key':        os.environ.get('AMAZON_AWS_ACCESS_KEY', ''),
        'aws_secret':     os.environ.get('AMAZON_AWS_SECRET_KEY', ''),
        'endpoint':       _ENDPOINT.get(mp_id, _DEFAULT_ENDPOINT),
        'region':         'us-east-1',
    }


# ── Orders ─────────────────────────────────────────────────────────────────────

def _sync_orders(org_id, token, creds):
    from datetime import datetime, timedelta, timezone
    since  = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
    params = {
        'MarketplaceIds':    creds['marketplace_id'],
        'CreatedAfter':      since,
        'MaxResultsPerPage': '100',
    }
    db           = get_db()
    count        = 0
    next_tok     = None
    has_fba_order = False  # True if any order shipped by Amazon (AFN)

    while True:
        resp    = _sp_get(
            creds['endpoint'], '/orders/v0/orders', token,
            params={'NextToken': next_tok} if next_tok else params,
            aws_key=creds['aws_key'], aws_secret=creds['aws_secret'],
            region=creds['region'])
        payload = resp.get('payload', {})
        orders  = payload.get('Orders', [])

        for o in orders:
            ext_id = o.get('AmazonOrderId', '')
            if not ext_id:
                continue
            status_map = {
                'Shipped': 'delivered', 'Delivered': 'delivered',
                'Unshipped': 'pending',  'Pending': 'pending',
                'Canceled': 'cancelled', 'Cancelled': 'cancelled',
            }
            status = status_map.get(o.get('OrderStatus', ''), 'delivered')
            try:
                gmv = float(o.get('OrderTotal', {}).get('Amount', 0) or 0)
            except (ValueError, TypeError):
                gmv = 0.0
            revenue = round(gmv * 0.85, 2)
            cost    = round(gmv * 0.40, 2)

            raw_date   = o.get('PurchaseDate', '')
            ordered_at = (raw_date[:19].replace('T', ' ')
                          if raw_date else
                          datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            buyer_email       = o.get('BuyerInfo', {}).get('BuyerEmail', '') or ''
            buyer_name        = o.get('BuyerInfo', {}).get('BuyerName',  '') or 'Amazon Customer'
            fulfill_channel   = o.get('FulfillmentChannel', 'MFN')  # AFN=FBA, MFN=FBM
            if fulfill_channel == 'AFN':
                has_fba_order = True
            contact_id        = None

            if buyer_email and '@' in buyer_email:
                row = db.execute(
                    'SELECT id FROM contacts WHERE org_id=? AND email=?',
                    (org_id, buyer_email)).fetchone()
                if row:
                    contact_id = row[0]
                else:
                    db.execute(
                        "INSERT INTO contacts "
                        "(org_id,name,email,source,rfm_segment,wa_opt_in,email_opt_in) "
                        "VALUES (?,?,?,'amazon','new',0,1)",
                        (org_id, buyer_name, buyer_email))
                    contact_id = db.execute(
                        'SELECT last_insert_rowid()').fetchone()[0]
                    db.commit()

            try:
                db.execute(
                    "INSERT INTO orders "
                    "(org_id,contact_id,marketplace,external_id,status,gmv,revenue,cost,channel,ordered_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(org_id,marketplace,external_id) DO UPDATE SET "
                    "status=excluded.status, gmv=excluded.gmv, "
                    "revenue=excluded.revenue, cost=excluded.cost",
                    (org_id, contact_id, 'amazon', ext_id, status,
                     gmv, revenue, cost, 'organic', ordered_at))
                count += 1
            except Exception as exc:
                print(f"[amazon] Order upsert {ext_id}: {exc}")

        next_tok = payload.get('NextToken')
        if not next_tok or not orders:
            break

    db.commit()
    db.close()

    # Write fulfillment_type to health table based on real order data
    _store_fulfillment_type(org_id, 'FBA' if has_fba_order else None)
    return count


def _store_fulfillment_type(org_id, fulfillment_type):
    """
    Store fulfillment_type in mp_account_health metrics_json.
    fulfillment_type: 'FBA', 'FBM', or None.
    None = only write FBM if nothing is stored yet (default).
    'FBA' / 'FBM' = always overwrite with the given value.
    """
    try:
        db = get_db()
        row = db.execute(
            "SELECT metrics_json FROM mp_account_health "
            "WHERE org_id=? AND platform='amazon'", (org_id,)).fetchone()
        db.close()
        current = json.loads(row['metrics_json'] if row else '{}') or {}
        existing = current.get('fulfillment_type')
        if fulfillment_type is None:
            if existing:  # Already have a value — don't overwrite
                return
            fulfillment_type = 'FBM'  # Default when no evidence either way
    except Exception:
        if fulfillment_type is None:
            fulfillment_type = 'FBM'
    try:
        db = get_db()
        row = db.execute(
            "SELECT metrics_json FROM mp_account_health "
            "WHERE org_id=? AND platform='amazon'", (org_id,)).fetchone()
        current = json.loads(row['metrics_json'] if row else '{}') or {}
        current['fulfillment_type'] = fulfillment_type
        db.execute(
            "INSERT INTO mp_account_health "
            "(org_id,platform,score,level,metrics_json,alerts_json) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(org_id,platform) DO UPDATE SET "
            "metrics_json=excluded.metrics_json",
            (org_id, 'amazon', current.get('score', 0), '', json.dumps(current), '[]'))
        db.commit()
        db.close()
        print(f'[amazon] fulfillment_type stored: {fulfillment_type}')
    except Exception as exc:
        print(f'[amazon] _store_fulfillment_type: {exc}')


# ── Products (Listings API → FBA inventory fallback) ──────────────────────────

def _sync_products(org_id, token, creds):
    endpoint    = creds['endpoint']
    seller_id   = creds['seller_id']
    marketplace = creds['marketplace_id']
    aws_key     = creds['aws_key']
    aws_secret  = creds['aws_secret']
    region      = creds['region']

    db    = get_db()
    count = 0

    # Try Listings Items API first
    resp  = _sp_get(
        endpoint, f'/listings/2021-08-01/items/{seller_id}', token,
        params={'marketplaceIds': marketplace,
                'includedData':   'summaries,offers',
                'pageSize':       '20'},
        aws_key=aws_key, aws_secret=aws_secret, region=region)
    items = resp.get('items', [])

    if not items:
        # Fallback: FBA Inventory — if this returns items, seller IS on FBA
        resp2 = _sp_get(
            endpoint, '/fba/inventory/v1/summaries', token,
            params={'granularityType': 'Marketplace',
                    'granularityId':   marketplace,
                    'marketplaceIds':  marketplace},
            aws_key=aws_key, aws_secret=aws_secret, region=region)
        fba_inv_items = resp2.get('payload', {}).get('inventorySummaries', [])
        if fba_inv_items:
            _store_fulfillment_type(org_id, 'FBA')  # Confirmed FBA via inventory
        for item in fba_inv_items:
            asin = item.get('asin', '')
            if not asin:
                continue
            try:
                db.execute(
                    "INSERT INTO mp_products "
                    "(org_id,platform,external_id,title,stock_qty,status) "
                    "VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(org_id,platform,external_id) DO UPDATE SET "
                    "title=excluded.title, stock_qty=excluded.stock_qty, "
                    "last_synced=datetime('now')",
                    (org_id, 'amazon', asin,
                     item.get('productName', asin),
                     item.get('totalQuantity', 0), 'active'))
                count += 1
            except Exception as exc:
                print(f"[amazon] FBA product {asin}: {exc}")
    else:
        for item in items:
            sums  = (item.get('summaries') or [{}])[0]
            asin  = sums.get('asin', item.get('sku', ''))
            title = sums.get('itemName', '')
            offers = item.get('offers') or []
            price  = 0.0
            if offers:
                try:
                    price = float(
                        offers[0].get('listingPrice', {}).get('amount', 0) or 0)
                except (ValueError, TypeError):
                    price = 0.0
            try:
                db.execute(
                    "INSERT INTO mp_products "
                    "(org_id,platform,external_id,title,price,status) "
                    "VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(org_id,platform,external_id) DO UPDATE SET "
                    "title=excluded.title, price=excluded.price, "
                    "last_synced=datetime('now')",
                    (org_id, 'amazon', asin, title, price, 'active'))
                count += 1
            except Exception as exc:
                print(f"[amazon] Listing {asin}: {exc}")

    db.commit()
    db.close()

    # Always check FBA inventory — independent of which API path was taken above
    # /fba/inventory/v1/summaries: if it returns ANY item → seller uses FBA
    try:
        resp_inv = _sp_get(
            creds['endpoint'], '/fba/inventory/v1/summaries', token,
            params={'granularityType': 'Marketplace',
                    'granularityId':   creds['marketplace_id'],
                    'marketplaceIds':  creds['marketplace_id']},
            aws_key=creds['aws_key'], aws_secret=creds['aws_secret'],
            region=creds['region'])
        inv_items = resp_inv.get('payload', {}).get('inventorySummaries', [])
        if inv_items:
            _store_fulfillment_type(org_id, 'FBA')
            print(f'[amazon] FBA confirmed via inventory ({len(inv_items)} items)')
        else:
            # Also check Listings API fulfillmentAvailability
            resp_fa = _sp_get(
                creds['endpoint'],
                f'/listings/2021-08-01/items/{creds["seller_id"]}', token,
                params={'marketplaceIds': creds['marketplace_id'],
                        'includedData':   'fulfillmentAvailability',
                        'pageSize':       '5'},
                aws_key=creds['aws_key'], aws_secret=creds['aws_secret'],
                region=creds['region'])
            for _item in resp_fa.get('items', []):
                for _avail in _item.get('fulfillmentAvailability', []):
                    if _avail.get('fulfillmentChannelCode', '').upper().startswith('AMAZON'):
                        _store_fulfillment_type(org_id, 'FBA')
                        print('[amazon] FBA confirmed via listing fulfillmentAvailability')
                        break
    except Exception as _fba_exc:
        print(f'[amazon] FBA detection error: {_fba_exc}')

    return count


# ── Account health ─────────────────────────────────────────────────────────────


def _check_fba_from_listings(endpoint, seller_id, marketplace, token,
                              aws_key, aws_secret, region):
    """Return True if seller has any FBA (AFN) active listing."""
    try:
        resp = _sp_get(
            endpoint, f'/listings/2021-08-01/items/{seller_id}', token,
            params={'marketplaceIds': marketplace,
                    'includedData':   'fulfillmentAvailability',
                    'pageSize':       '10'},
            aws_key=aws_key, aws_secret=aws_secret, region=region)
        for item in resp.get('items', []):
            for avail in item.get('fulfillmentAvailability', []):
                if avail.get('fulfillmentChannelCode', '').startswith('AMAZON'):
                    return True
    except Exception as exc:
        print(f'[amazon] FBA check error: {exc}')
    return False



def _detect_fba_multilayer(org_id, token, creds):
    """
    Detect FBA using 4 independent layers (first success wins):
      Layer 1: config_json has is_fba=True (manual override by user)
      Layer 2: /fba/inventory/v1/summaries returns items (scope: fulfillment_inbound)
      Layer 3: /listings fulfillmentAvailability has AMAZON channel (scope: listings_items_read)
      Layer 4: lateShipmentRate is 0/null in health API (FBA = Amazon ships, rate is 0)
    Returns 'FBA' or 'FBM'.
    """
    endpoint   = creds['endpoint']
    marketplace = creds['marketplace_id']
    seller_id   = creds['seller_id']
    aws_key    = creds['aws_key']
    aws_secret = creds['aws_secret']
    region     = creds['region']

    # ── Layer 1: Manual override in integration config ─────────────────────
    try:
        from oauth_manager import get_integration
        integ = get_integration(org_id, 'amazon')
        if integ and integ.get('config', {}).get('is_fba'):
            print('[amazon] FBA confirmed: manual override in config')
            return 'FBA'
    except Exception:
        pass

    # ── Layer 2: FBA Inventory API ─────────────────────────────────────────
    try:
        resp = _sp_get(
            endpoint, '/fba/inventory/v1/summaries', token,
            params={'granularityType': 'Marketplace',
                    'granularityId':   marketplace,
                    'marketplaceIds':  marketplace,
                    'details':         'false'},
            aws_key=aws_key, aws_secret=aws_secret, region=region)
        inv = resp.get('payload', {}).get('inventorySummaries', [])
        if inv:
            print(f'[amazon] FBA confirmed: {len(inv)} items in FBA inventory')
            return 'FBA'
    except Exception as exc:
        print(f'[amazon] FBA inventory check: {exc}')

    # ── Layer 3: Listings API with fulfillmentAvailability ─────────────────
    try:
        resp2 = _sp_get(
            endpoint, f'/listings/2021-08-01/items/{seller_id}', token,
            params={'marketplaceIds': marketplace,
                    'includedData':   'fulfillmentAvailability',
                    'pageSize':       '5'},
            aws_key=aws_key, aws_secret=aws_secret, region=region)
        for item in resp2.get('items', []):
            for avail in item.get('fulfillmentAvailability', []):
                ch = avail.get('fulfillmentChannelCode', '').upper()
                if ch.startswith('AMAZON') or ch == 'AFN':
                    print(f'[amazon] FBA confirmed: fulfillmentChannelCode={ch}')
                    return 'FBA'
    except Exception as exc:
        print(f'[amazon] Listings fulfillmentAvailability check: {exc}')

    # ── Layer 4: Heuristic — lateShipmentRate == 0 suggests FBA ───────────
    # FBA sellers: Amazon ships → lateShipmentRate typically 0 or not tracked
    try:
        resp3 = _sp_get(endpoint, '/seller/v1/account/health', token,
                        aws_key=aws_key, aws_secret=aws_secret, region=region)
        agg3 = resp3.get('payload', {}).get('aggregated', {})
        lsr_raw = agg3.get('lateShipmentRate', {})
        lsr_val = lsr_raw.get('value')
        # FBA sellers: lsr is None, absent, or 0.0
        if lsr_val is None or lsr_raw == {} or (
                isinstance(lsr_val, (int, float)) and float(lsr_val) == 0.0):
            print(f'[amazon] FBA likely: lateShipmentRate={lsr_val} (FBA=Amazon ships)')
            return 'FBA'
    except Exception as exc:
        print(f'[amazon] lateShipmentRate heuristic: {exc}')

    # ── Preserve existing value before defaulting to FBM ──────────────────
    try:
        db_p = get_db()
        row_p = db_p.execute(
            "SELECT metrics_json FROM mp_account_health "
            "WHERE org_id=? AND platform='amazon'", (org_id,)).fetchone()
        db_p.close()
        existing = json.loads(row_p['metrics_json'] if row_p else '{}') or {}
        if existing.get('fulfillment_type') == 'FBA':
            print('[amazon] FBA preserved from previous sync')
            return 'FBA'
    except Exception:
        pass

    print('[amazon] FBA not detected — defaulting to FBM')
    return 'FBM'


def _sync_account_health(org_id, token, creds):
    endpoint    = creds['endpoint']
    marketplace = creds['marketplace_id']
    aws_key     = creds['aws_key']
    aws_secret  = creds['aws_secret']
    region      = creds['region']

    resp    = _sp_get(
        endpoint, '/seller/v1/account/health/ratings', token,
        params={'marketplaceIds': marketplace},
        aws_key=aws_key, aws_secret=aws_secret, region=region)
    payload = resp.get('payload', {})
    rating  = payload.get('overallPerformanceRating', 'HEALTHY')
    issues  = payload.get('issues', [])

    score_map = {'EXCELLENT': 95, 'GOOD': 85, 'FAIR': 65,
                 'AT_RISK': 40,   'CRITICAL': 20, 'HEALTHY': 90}
    score  = score_map.get(rating, 80)
    level  = rating.replace('_', ' ').title()
    alerts = [{'type': 'warning', 'message': i.get('title', '')}
              for i in issues if i.get('impact') == 'HIGH']

    # ODR / late shipment metrics
    metrics = {}
    resp2   = _sp_get(endpoint, '/seller/v1/account/health', token,
                      aws_key=aws_key, aws_secret=aws_secret, region=region)
    agg = resp2.get('payload', {}).get('aggregated', {})
    if agg:
        metrics['order_defect_rate']  = agg.get('orderDefectRate',  {}).get('value', 0)
        metrics['late_shipment_rate'] = agg.get('lateShipmentRate', {}).get('value', 0)
        metrics['cancel_rate']        = agg.get('canceledRate',     {}).get('value', 0)

    # Preserve fulfillment_type set by _sync_orders (AFN order detection)
    db_chk = get_db()
    chk = db_chk.execute(
        "SELECT metrics_json FROM mp_account_health "
        "WHERE org_id=? AND platform='amazon'", (org_id,)).fetchone()
    db_chk.close()
    existing = json.loads(chk['metrics_json'] if chk else '{}') or {}
    metrics['fulfillment_type'] = existing.get('fulfillment_type', 'FBM')

    db = get_db()
    try:
        db.execute(
            "INSERT INTO mp_account_health "
            "(org_id,platform,score,level,metrics_json,alerts_json) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(org_id,platform) DO UPDATE SET "
            "score=excluded.score, level=excluded.level, "
            "metrics_json=excluded.metrics_json, alerts_json=excluded.alerts_json, "
            "last_synced=datetime('now')",
            (org_id, 'amazon', score, level,
             json.dumps(metrics), json.dumps(alerts)))
        db.commit()
    finally:
        db.close()
    return 1


# ── Returns (derived from synced orders) ──────────────────────────────────────

def _sync_returns(org_id):
    db  = get_db()
    row = db.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN status IN ('cancelled','returned') THEN 1 ELSE 0 END) AS returns, "
        "SUM(CASE WHEN status IN ('cancelled','returned') THEN revenue ELSE 0 END) AS ref_rev "
        "FROM orders WHERE org_id=? AND marketplace='amazon'",
        (org_id,)).fetchone()

    total   = row['total']   or 0
    returns = row['returns'] or 0
    ref_rev = row['ref_rev'] or 0
    rate    = round(returns / max(total, 1) * 100, 2)

    db.execute(
        "INSERT INTO mp_returns "
        "(org_id,platform,total_orders,total_returns,return_rate,refunded_revenue,trend) "
        "VALUES (?,?,?,?,?,?,?) "
        "ON CONFLICT(org_id,platform) DO UPDATE SET "
        "total_orders=excluded.total_orders, total_returns=excluded.total_returns, "
        "return_rate=excluded.return_rate, refunded_revenue=excluded.refunded_revenue, "
        "trend=excluded.trend, last_synced=datetime('now')",
        (org_id, 'amazon', total, returns, rate, ref_rev,
         'rising' if rate > 5 else 'stable'))
    db.commit()
    db.close()
    return 1



# ── Competitors (Amazon Catalog API) ─────────────────────────────────────────

def _sync_amazon_competitors(org_id, token, creds):
    """
    Search for competing products via Amazon Catalog Items API.
    Uses keywords from our own product titles stored in mp_products.
    Falls back to category search if no products found.
    """
    endpoint    = creds['endpoint']
    marketplace = creds['marketplace_id']
    aws_key     = creds['aws_key']
    aws_secret  = creds['aws_secret']
    region      = creds['region']
    seller_id   = creds['seller_id']

    # Get keywords from our own products
    db = get_db()
    our_products = db.execute(
        "SELECT title FROM mp_products WHERE org_id=? AND platform='amazon' LIMIT 3",
        (org_id,)).fetchall()
    db.close()

    keywords = []
    for p in our_products:
        title = (p['title'] or '')[:40]  # First 40 chars of title
        if title:
            keywords.append(title)

    if not keywords:
        keywords = ['cooler', 'caixa termica']  # Generic fallback

    saved = 0
    seen_asins = set()

    for kw in keywords[:2]:  # Max 2 keyword searches to avoid rate limits
        try:
            resp = _sp_get(
                endpoint, '/catalog/2022-04-01/items', token,
                params={
                    'keywords':        kw,
                    'marketplaceIds':  marketplace,
                    'includedData':    'summaries,salesRanks,relationships',
                    'pageSize':        '5',
                },
                aws_key=aws_key, aws_secret=aws_secret, region=region)

            items = resp.get('items', [])
            db2 = get_db()
            for item in items:
                sums  = (item.get('summaries') or [{}])[0]
                asin  = item.get('asin', '')
                if not asin or asin in seen_asins:
                    continue
                seen_asins.add(asin)

                title        = sums.get('itemName', asin)[:120]
                brand        = sums.get('brand', '')
                class_group  = sums.get('productType', '')
                # Rating/reviews not in summaries — use defaults
                rating       = 4.2
                reviews      = 0

                # Try to get price from our product prices for comparison
                price = 0.0

                badge = 'Amazon Choice' if len(seen_asins) == 1 else ''

                try:
                    db2.execute("""
                        INSERT INTO mp_competitors
                            (org_id, platform, seller_id, nickname, rating,
                             completed_sales, price, stock, badge,
                             fulfillment, sponsored, sold_qty, power_status, last_synced)
                        VALUES (?, 'amazon', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                        ON CONFLICT(org_id, platform, seller_id) DO UPDATE SET
                            nickname=excluded.nickname,
                            rating=excluded.rating,
                            price=excluded.price,
                            badge=excluded.badge,
                            last_synced=datetime('now')
                    """, (org_id, asin, title, rating, reviews,
                          price, 50, badge, 1, 1, reviews, brand))
                    saved += 1
                except Exception as exc:
                    print(f'[amazon] competitor upsert {asin}: {exc}')
            db2.commit()
            db2.close()

        except Exception as exc:
            print(f'[amazon] competitor search "{kw}": {exc}')

    print(f'[amazon] {saved} competitors synced')
    return saved


# ── Entry point ────────────────────────────────────────────────────────────────

def sync_all(org_id):
    """Full Amazon SP-API sync. Returns total records synced."""
    print(f"[amazon_sync] org_id={org_id} — starting")

    creds = _load_creds(org_id)
    if not creds:
        print(f"[amazon_sync] No connected Amazon for org_id={org_id}")
        return 0
    if not creds['client_id'] or not creds['refresh_token']:
        print("[amazon_sync] Missing client_id or refresh_token in config")
        return 0

    if not creds['aws_key']:
        print("[amazon_sync] WARNING: AMAZON_AWS_ACCESS_KEY not set — "
              "SP-API SigV4 signing disabled. Add env var for full access.")

    # 1. LWA token
    try:
        access_token, _ = _get_lwa_token(
            creds['client_id'], creds['client_secret'], creds['refresh_token'])
        print(f"[amazon_sync] LWA token: {access_token[:12]}…")
    except AuthError as exc:
        print(f"[amazon_sync] LWA failed: {exc}")
        db = get_db()
        db.execute(
            "UPDATE api_integrations SET status='token_expired' "
            "WHERE org_id=? AND platform='amazon'", (org_id,))
        db.commit(); db.close()
        return 0
    except Exception as exc:
        print(f"[amazon_sync] LWA error: {exc}")
        return 0

    total = 0

    # 2. Orders
    try:
        n = _sync_orders(org_id, access_token, creds)
        print(f"[amazon_sync] {n} orders synced")
        total += n
    except AuthError:
        print("[amazon_sync] Auth error on orders"); return total
    except Exception as exc:
        print(f"[amazon_sync] Orders error: {exc}")

    # 3. Products / listings
    try:
        n = _sync_products(org_id, access_token, creds)
        print(f"[amazon_sync] {n} products synced")
        total += n
    except Exception as exc:
        print(f"[amazon_sync] Products error: {exc}")

    # 4. Account health
    try:
        _sync_account_health(org_id, access_token, creds)
        print("[amazon_sync] Account health synced")
        total += 1
    except Exception as exc:
        print(f"[amazon_sync] Health error: {exc}")

    # 5. Returns (computed)
    try:
        _sync_returns(org_id)
        total += 1
    except Exception as exc:
        print(f"[amazon_sync] Returns error: {exc}")

    # 6. Competitors (Catalog API)
    try:
        n = _sync_amazon_competitors(org_id, access_token, creds)
        total += n
    except Exception as exc:
        print(f'[amazon_sync] Competitors error: {exc}')

    # 7. Update last_sync
    db = get_db()
    db.execute(
        "UPDATE api_integrations SET last_sync=datetime('now') "
        "WHERE org_id=? AND platform='amazon'", (org_id,))
    db.commit(); db.close()

    print(f"[amazon_sync] Done — {total} records total")
    return total
