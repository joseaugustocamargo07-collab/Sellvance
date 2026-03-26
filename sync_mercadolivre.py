"""
Sellvance -- Mercado Livre Data Sync (v2 - full schema fix)
Syncs account health, orders, products, returns, ads, and competitors
from api.mercadolibre.com into local SQLite tables.
"""

import json
import logging
from database import get_db
from sync_base import get_valid_token, api_request, run_sync_if_needed

logger = logging.getLogger(__name__)

ML_API = 'https://api.mercadolibre.com'


# ── Table creation ───────────────────────────────────────────────────────────

def _ensure_tables():
    """Create ML-specific tables with ALL columns the app expects."""
    db = get_db()
    try:
        db.executescript('''
            CREATE TABLE IF NOT EXISTS mp_account_health (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id              INTEGER NOT NULL,
                platform            TEXT NOT NULL DEFAULT 'mercado_livre',
                power_seller_status TEXT DEFAULT '',
                positive_ratings    REAL DEFAULT 0,
                negative_ratings    REAL DEFAULT 0,
                neutral_ratings     REAL DEFAULT 0,
                claims_rate         REAL DEFAULT 0,
                delayed_rate        REAL DEFAULT 0,
                cancellations_rate  REAL DEFAULT 0,
                score               INTEGER DEFAULT 0,
                level               TEXT DEFAULT '',
                metrics_json        TEXT DEFAULT '{}',
                alerts_json         TEXT DEFAULT '[]',
                updated_at          TEXT DEFAULT (datetime('now')),
                UNIQUE(org_id, platform)
            );

            CREATE TABLE IF NOT EXISTS mp_products (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id          INTEGER NOT NULL,
                platform        TEXT NOT NULL DEFAULT 'mercado_livre',
                external_id     TEXT NOT NULL,
                title           TEXT DEFAULT '',
                price           REAL DEFAULT 0,
                stock_qty       INTEGER DEFAULT 0,
                sold_qty        INTEGER DEFAULT 0,
                status          TEXT DEFAULT '',
                category        TEXT DEFAULT '',
                thumbnail_url   TEXT DEFAULT '',
                listing_type    TEXT DEFAULT '',
                listing_type_id TEXT DEFAULT '',
                updated_at      TEXT DEFAULT (datetime('now')),
                UNIQUE(org_id, platform, external_id)
            );

            CREATE TABLE IF NOT EXISTS mp_returns (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id              INTEGER NOT NULL,
                platform            TEXT NOT NULL DEFAULT 'mercado_livre',
                total_orders        INTEGER DEFAULT 0,
                total_returns       INTEGER DEFAULT 0,
                returned_orders     INTEGER DEFAULT 0,
                return_rate         REAL DEFAULT 0,
                reasons_json        TEXT DEFAULT '[]',
                avg_resolution_days REAL DEFAULT 0,
                refunded_revenue    REAL DEFAULT 0,
                trend               TEXT DEFAULT 'stable',
                updated_at          TEXT DEFAULT (datetime('now')),
                UNIQUE(org_id, platform)
            );

            CREATE TABLE IF NOT EXISTS mp_ads (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id          INTEGER NOT NULL,
                platform        TEXT NOT NULL DEFAULT 'mercado_livre',
                external_id     TEXT NOT NULL,
                title           TEXT DEFAULT '',
                listing_type_id TEXT DEFAULT '',
                listing_type    TEXT DEFAULT '',
                visits_30d      INTEGER DEFAULT 0,
                price           REAL DEFAULT 0,
                spend           REAL DEFAULT 0,
                revenue         REAL DEFAULT 0,
                clicks          INTEGER DEFAULT 0,
                impressions     INTEGER DEFAULT 0,
                conversions     INTEGER DEFAULT 0,
                ctr             REAL DEFAULT 0,
                updated_at      TEXT DEFAULT (datetime('now')),
                UNIQUE(org_id, platform, external_id)
            );

            CREATE TABLE IF NOT EXISTS mp_competitors (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id          INTEGER NOT NULL,
                platform        TEXT NOT NULL DEFAULT 'mercado_livre',
                seller_id       TEXT NOT NULL,
                nickname        TEXT DEFAULT '',
                rating          REAL DEFAULT 0,
                completed_sales INTEGER DEFAULT 0,
                price           REAL DEFAULT 0,
                stock           INTEGER DEFAULT 0,
                badge           TEXT DEFAULT '',
                fulfillment     INTEGER DEFAULT 0,
                sponsored       INTEGER DEFAULT 0,
                sold_qty        INTEGER DEFAULT 0,
                power_status    TEXT DEFAULT '',
                last_synced     TEXT DEFAULT (datetime('now')),
                UNIQUE(org_id, platform, seller_id)
            );
        ''')
        db.commit()
    except Exception as e:
        logger.warning("Table creation warning (may already exist with old schema): %s", e)
        db.commit()
    finally:
        db.close()

    # Add missing columns to existing tables (safe ALTER)
    _migrate_columns()


def _migrate_columns():
    """Add missing columns to existing tables and rebuild constraints if needed."""
    db = get_db()
    migrations = [
        ("mp_account_health", "platform",    "TEXT NOT NULL DEFAULT 'mercado_livre'"),
        ("mp_account_health", "level",       "TEXT DEFAULT ''"),
        ("mp_account_health", "metrics_json","TEXT DEFAULT '{}'"),
        ("mp_account_health", "alerts_json", "TEXT DEFAULT '[]'"),
        ("mp_products",       "platform",    "TEXT NOT NULL DEFAULT 'mercado_livre'"),
        ("mp_products",       "category",    "TEXT DEFAULT ''"),
        ("mp_products",       "listing_type","TEXT DEFAULT ''"),
        ("mp_returns",        "platform",    "TEXT NOT NULL DEFAULT 'mercado_livre'"),
        ("mp_returns",        "total_returns","INTEGER DEFAULT 0"),
        ("mp_returns",        "reasons_json","TEXT DEFAULT '[]'"),
        ("mp_returns",        "avg_resolution_days","REAL DEFAULT 0"),
        ("mp_returns",        "refunded_revenue","REAL DEFAULT 0"),
        ("mp_returns",        "trend",       "TEXT DEFAULT 'stable'"),
        ("mp_ads",            "platform",    "TEXT NOT NULL DEFAULT 'mercado_livre'"),
        ("mp_ads",            "listing_type","TEXT DEFAULT ''"),
        ("mp_ads",            "spend",       "REAL DEFAULT 0"),
        ("mp_ads",            "revenue",     "REAL DEFAULT 0"),
        ("mp_ads",            "clicks",      "INTEGER DEFAULT 0"),
        ("mp_ads",            "impressions", "INTEGER DEFAULT 0"),
        ("mp_ads",            "conversions", "INTEGER DEFAULT 0"),
        ("mp_ads",            "ctr",         "REAL DEFAULT 0"),
    ]
    for table, col, typedef in migrations:
        try:
            db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")
        except Exception:
            pass  # Column already exists

    # Create indexes for platform queries (if not exist)
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_mp_health_org_plat ON mp_account_health(org_id, platform)",
        "CREATE INDEX IF NOT EXISTS idx_mp_products_org_plat ON mp_products(org_id, platform)",
        "CREATE INDEX IF NOT EXISTS idx_mp_returns_org_plat ON mp_returns(org_id, platform)",
        "CREATE INDEX IF NOT EXISTS idx_mp_ads_org_plat ON mp_ads(org_id, platform)",
    ]:
        try:
            db.execute(idx_sql)
        except Exception:
            pass

    db.commit()
    db.close()


_ensure_tables()


# ── Auth header helper ───────────────────────────────────────────────────────

def _auth(token):
    """Return Authorization header dict."""
    return {'Authorization': f'Bearer {token}'}


# ── Sub-sync: Account Health ────────────────────────────────────────────────

def sync_account_health(org_id, token):
    """Fetch seller reputation from /users/me and upsert mp_account_health."""
    data = api_request(f'{ML_API}/users/me', headers=_auth(token))

    rep = data.get('seller_reputation', {})
    pss = rep.get('power_seller_status', '') or ''

    transactions = rep.get('transactions', {})
    ratings = transactions.get('ratings', {})
    positive = ratings.get('positive', 0)
    negative = ratings.get('negative', 0)
    neutral = ratings.get('neutral', 0)
    completed = transactions.get('completed', 0)

    metrics = rep.get('metrics', {})
    claims_rate = _metric_rate(metrics, 'claims')
    delayed_rate = _metric_rate(metrics, 'delayed_handling_time')
    cancel_rate = _metric_rate(metrics, 'cancellations')

    # Compute a 0-100 health score
    score = 100
    score -= int(claims_rate * 200)
    score -= int(delayed_rate * 200)
    score -= int(cancel_rate * 200)
    score -= int(negative * 50)
    if pss == 'platinum':
        score = min(100, score + 5)
    elif pss == 'gold':
        score = min(100, score + 3)
    score = max(0, min(100, score))

    # Level label
    level_map = {
        'platinum': 'MercadoLider Platinum',
        'gold': 'MercadoLider Gold',
        'silver': 'MercadoLider',
    }
    level = level_map.get(pss, 'Seller Standard')

    # Build structured metrics JSON for the frontend
    metrics_json = json.dumps({
        'reputation': f"{round(positive * 100)}%",
        'completed_sales': completed,
        'claims_rate': f"{round(claims_rate * 100, 1)}%",
        'delayed_rate': f"{round(delayed_rate * 100, 1)}%",
        'cancellations_rate': f"{round(cancel_rate * 100, 1)}%",
        'power_seller_status': pss or 'none',
    })

    # Build alerts
    alerts = []
    if claims_rate > 0.03:
        alerts.append('⚠️ Taxa de reclamacoes alta — pode afetar exposicao dos anuncios')
    if delayed_rate > 0.05:
        alerts.append('⚠️ Entregas atrasadas acima de 5% — melhore logistica')
    if cancel_rate > 0.025:
        alerts.append('⚠️ Taxa de cancelamento alta — verificar estoque e processamento')
    if pss in ('platinum', 'gold'):
        alerts.append(f'✅ Status {level} ativo — voce tem exposicao prioritaria')
    if positive >= 0.95:
        alerts.append('✅ Reputacao excelente — mantenha o padrao')
    elif positive < 0.8:
        alerts.append('🔴 Reputacao abaixo de 80% — risco de perda de visibilidade')
    alerts_json = json.dumps(alerts)

    db = get_db()
    try:
        db.execute('''
            INSERT INTO mp_account_health
                (org_id, platform, power_seller_status, positive_ratings, negative_ratings,
                 neutral_ratings, claims_rate, delayed_rate, cancellations_rate,
                 score, level, metrics_json, alerts_json, updated_at)
            VALUES (?, 'mercado_livre', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(org_id) DO UPDATE SET
                platform            = 'mercado_livre',
                power_seller_status = excluded.power_seller_status,
                positive_ratings    = excluded.positive_ratings,
                negative_ratings    = excluded.negative_ratings,
                neutral_ratings     = excluded.neutral_ratings,
                claims_rate         = excluded.claims_rate,
                delayed_rate        = excluded.delayed_rate,
                cancellations_rate  = excluded.cancellations_rate,
                score               = excluded.score,
                level               = excluded.level,
                metrics_json        = excluded.metrics_json,
                alerts_json         = excluded.alerts_json,
                updated_at          = datetime('now')
        ''', (org_id, pss, positive, negative, neutral,
              claims_rate, delayed_rate, cancel_rate, score, level,
              metrics_json, alerts_json))
        db.commit()
    finally:
        db.close()

    logger.info("Synced account health for org=%s score=%d level=%s", org_id, score, level)
    return 1


def _metric_rate(metrics, key):
    """Safely extract a rate from ML metrics dict."""
    try:
        return float(metrics.get(key, {}).get('rate', 0))
    except (TypeError, ValueError):
        return 0.0


# ── Sub-sync: Orders ────────────────────────────────────────────────────────

def sync_orders(org_id, token, user_id):
    """Paginate through /orders/search and insert new orders.
    Only counts PAID orders as revenue. Cancelled/returned = R$0 revenue."""
    offset = 0
    limit = 50
    total_synced = 0

    # ML order statuses that represent actual completed sales
    PAID_STATUSES = {'paid', 'delivered', 'shipped'}

    while True:
        url = (
            f'{ML_API}/orders/search'
            f'?seller={user_id}&sort=date_desc&limit={limit}&offset={offset}'
        )
        try:
            data = api_request(url, headers=_auth(token))
        except RuntimeError as e:
            logger.error("Failed to fetch orders at offset %d: %s", offset, e)
            break

        results = data.get('results', [])
        if not results:
            break

        db = get_db()
        try:
            for order in results:
                ext_id = str(order.get('id', ''))
                if not ext_id:
                    continue

                total_amount = float(order.get('total_amount', 0))
                paid_amount = float(order.get('paid_amount', 0))
                status = order.get('status', '')
                date_created = order.get('date_created', '')
                ordered_at = _normalize_datetime(date_created)

                # GMV = paid_amount (includes shipping) — best total for the order
                # Falls back to total_amount if paid_amount is 0
                gmv = paid_amount if paid_amount > 0 else total_amount

                # Revenue = gmv for paid orders, 0 for cancelled/returned
                if status in PAID_STATUSES:
                    revenue = gmv
                else:
                    revenue = 0.0

                db.execute('''
                    INSERT OR IGNORE INTO orders
                        (org_id, marketplace, external_id, status, gmv, revenue, channel, ordered_at)
                    VALUES (?, 'mercado_livre', ?, ?, ?, ?, 'marketplace', ?)
                ''', (org_id, ext_id, status, gmv, revenue, ordered_at))

                # Always update existing orders (status may have changed)
                db.execute('''
                    UPDATE orders SET status=?, gmv=?, revenue=?
                    WHERE org_id=? AND marketplace='mercado_livre' AND external_id=?
                ''', (status, gmv, revenue, org_id, ext_id))

                buyer = order.get('buyer', {})
                _upsert_contact_from_buyer(db, org_id, buyer)

                total_synced += 1

            db.commit()
        finally:
            db.close()

        paging = data.get('paging', {})
        total_results = paging.get('total', 0)
        offset += limit
        if offset >= total_results:
            break

    logger.info("Synced %d orders for org=%s", total_synced, org_id)
    return total_synced


def _upsert_contact_from_buyer(db, org_id, buyer):
    """Create or update a contact from ML buyer data."""
    if not buyer:
        return
    nickname = buyer.get('nickname', '')
    first_name = buyer.get('first_name', '')
    last_name = buyer.get('last_name', '')
    email = buyer.get('email', '')
    phone_info = buyer.get('phone', {})
    phone = phone_info.get('number', '') if isinstance(phone_info, dict) else ''

    name = f"{first_name} {last_name}".strip() or nickname
    if not name or not email:
        return

    existing = db.execute(
        'SELECT id FROM contacts WHERE org_id = ? AND email = ?',
        (org_id, email)
    ).fetchone()

    if not existing:
        db.execute('''
            INSERT INTO contacts (org_id, name, email, phone, source, rfm_segment)
            VALUES (?, ?, ?, ?, 'mercado_livre', 'new')
        ''', (org_id, name, email, phone))


def _normalize_datetime(iso_str):
    """Convert ISO 8601 datetime to 'YYYY-MM-DD HH:MM:SS'."""
    if not iso_str:
        return ''
    try:
        clean = iso_str.replace('T', ' ')
        for sep in ('+', '.'):
            idx = clean.find(sep, 10)
            if idx > 0:
                clean = clean[:idx]
        return clean[:19]
    except Exception:
        return iso_str[:19] if len(iso_str) >= 19 else iso_str


# ── Sub-sync: Products ──────────────────────────────────────────────────────

def sync_products(org_id, token, user_id):
    """Fetch all seller items and upsert into mp_products and stock_items."""
    total_synced = 0
    offset = 0
    limit = 50

    while True:
        url = f'{ML_API}/users/{user_id}/items/search?limit={limit}&offset={offset}'
        try:
            search_data = api_request(url, headers=_auth(token))
        except RuntimeError as e:
            logger.error("Failed to search items at offset %d: %s", offset, e)
            break

        item_ids = search_data.get('results', [])
        if not item_ids:
            break

        for i in range(0, len(item_ids), 20):
            batch = item_ids[i:i + 20]
            ids_param = ','.join(batch)
            multi_url = f'{ML_API}/items?ids={ids_param}'
            try:
                items_data = api_request(multi_url, headers=_auth(token))
            except RuntimeError as e:
                logger.error("Failed to multi-get items: %s", e)
                continue

            db = get_db()
            try:
                for item_wrapper in items_data:
                    if isinstance(item_wrapper, dict) and 'body' in item_wrapper:
                        item = item_wrapper.get('body', {})
                        if item_wrapper.get('code', 200) != 200:
                            continue
                    else:
                        item = item_wrapper

                    ext_id = str(item.get('id', ''))
                    if not ext_id:
                        continue

                    title = item.get('title', '')
                    price = float(item.get('price', 0) or 0)
                    available_qty = int(item.get('available_quantity', 0) or 0)
                    sold_qty = int(item.get('sold_quantity', 0) or 0)
                    status = item.get('status', '')
                    thumbnail = item.get('thumbnail', '') or item.get('secure_thumbnail', '')
                    listing_type = item.get('listing_type_id', '')
                    category_id = item.get('category_id', '')

                    db.execute('''
                        INSERT INTO mp_products
                            (org_id, platform, external_id, title, price, stock_qty, sold_qty,
                             status, category, thumbnail_url, listing_type, listing_type_id, updated_at)
                        VALUES (?, 'mercado_livre', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                        ON CONFLICT(org_id, external_id) DO UPDATE SET
                            platform        = 'mercado_livre',
                            title           = excluded.title,
                            price           = excluded.price,
                            stock_qty       = excluded.stock_qty,
                            sold_qty        = excluded.sold_qty,
                            status          = excluded.status,
                            category        = excluded.category,
                            thumbnail_url   = excluded.thumbnail_url,
                            listing_type    = excluded.listing_type,
                            listing_type_id = excluded.listing_type_id,
                            updated_at      = datetime('now')
                    ''', (org_id, ext_id, title, price, available_qty,
                          sold_qty, status, category_id, thumbnail, listing_type, listing_type))

                    _upsert_stock_item(db, org_id, ext_id, title, price, available_qty)
                    total_synced += 1

                db.commit()
            finally:
                db.close()

        paging = search_data.get('paging', {})
        total_results = paging.get('total', 0)
        offset += limit
        if offset >= total_results:
            break

    logger.info("Synced %d products for org=%s", total_synced, org_id)
    return total_synced


def _upsert_stock_item(db, org_id, sku, name, price, qty):
    """Upsert a stock_items row from ML product data."""
    existing = db.execute(
        'SELECT id FROM stock_items WHERE org_id = ? AND sku = ?',
        (org_id, sku)
    ).fetchone()

    if existing:
        db.execute('''
            UPDATE stock_items
            SET name = ?, sale_price = ?, stock_qty = ?, last_updated = datetime('now')
            WHERE org_id = ? AND sku = ?
        ''', (name, price, qty, org_id, sku))
    else:
        db.execute('''
            INSERT INTO stock_items
                (org_id, sku, name, marketplace, stock_qty, sale_price, status, last_updated)
            VALUES (?, ?, ?, 'mercado_livre', ?, ?, ?, datetime('now'))
        ''', (org_id, sku, name, qty, price,
              'out' if qty == 0 else 'critical' if qty < 5 else 'low' if qty < 15 else 'ok'))


# ── Sub-sync: Returns ───────────────────────────────────────────────────────

def sync_returns(org_id, token, user_id):
    """Compute return rate from order statuses and store in mp_returns."""
    total_orders = 0
    returned_orders = 0
    refunded_total = 0.0
    reason_counts = {}
    offset = 0
    limit = 50

    while True:
        url = (
            f'{ML_API}/orders/search'
            f'?seller={user_id}&sort=date_desc&limit={limit}&offset={offset}'
        )
        try:
            data = api_request(url, headers=_auth(token))
        except RuntimeError as e:
            logger.error("Failed to fetch orders for returns at offset %d: %s", offset, e)
            break

        results = data.get('results', [])
        if not results:
            break

        for order in results:
            total_orders += 1
            status = order.get('status', '')
            if status in ('cancelled', 'returned'):
                returned_orders += 1
                refunded_total += float(order.get('total_amount', 0))
                # Try to get cancellation reason
                reason = order.get('cancel_detail', {}).get('reason', '') if isinstance(order.get('cancel_detail'), dict) else ''
                if not reason:
                    reason = 'Motivo nao informado'
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

        paging = data.get('paging', {})
        total_results = paging.get('total', 0)
        offset += limit
        if offset >= total_results or offset >= 200:  # Limit to 200 orders for performance
            break

    return_rate = round(returned_orders / max(total_orders, 1) * 100, 2)

    # Build reasons list
    reasons = []
    for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True):
        pct = round(count / max(returned_orders, 1) * 100, 1)
        reasons.append({'reason': reason, 'count': count, 'pct': pct})
    reasons_json = json.dumps(reasons[:10])

    db = get_db()
    try:
        db.execute('''
            INSERT INTO mp_returns
                (org_id, platform, total_orders, total_returns, returned_orders,
                 return_rate, reasons_json, refunded_revenue, trend, updated_at)
            VALUES (?, 'mercado_livre', ?, ?, ?, ?, ?, ?, 'stable', datetime('now'))
            ON CONFLICT(org_id) DO UPDATE SET
                platform        = 'mercado_livre',
                total_orders    = excluded.total_orders,
                total_returns   = excluded.total_returns,
                returned_orders = excluded.returned_orders,
                return_rate     = excluded.return_rate,
                reasons_json    = excluded.reasons_json,
                refunded_revenue= excluded.refunded_revenue,
                updated_at      = datetime('now')
        ''', (org_id, total_orders, returned_orders, returned_orders,
              return_rate, reasons_json, refunded_total))
        db.commit()
    finally:
        db.close()

    logger.info("Synced returns for org=%s: %d/%d = %.1f%%",
                org_id, returned_orders, total_orders, return_rate)
    return 1


# ── Sub-sync: Ads (promoted listings) ───────────────────────────────────────

def sync_ads(org_id, token, user_id):
    """Identify promoted items (gold_special/gold_pro) and fetch visit data."""
    db = get_db()
    try:
        products = db.execute('''
            SELECT external_id, title, listing_type_id, listing_type, price, sold_qty
            FROM mp_products
            WHERE org_id = ? AND platform = 'mercado_livre'
            AND listing_type_id IN ('gold_special', 'gold_pro', 'gold_premium')
        ''', (org_id,)).fetchall()
    finally:
        db.close()

    total_synced = 0

    for product in products:
        p = dict(product)
        ext_id = p['external_id']
        title = p['title']
        listing_type = p.get('listing_type', '') or p.get('listing_type_id', '')
        price = p['price']

        # Get visit data for last 30 days
        visits_30d = 0
        visits_url = f'{ML_API}/items/{ext_id}/visits/time_window?last=30&unit=day'
        try:
            visits_data = api_request(visits_url, headers=_auth(token))
            if isinstance(visits_data, list):
                visits_30d = sum(
                    int(d.get('total', 0)) for d in visits_data
                    if isinstance(d, dict)
                )
            elif isinstance(visits_data, dict):
                visits_30d = int(visits_data.get('total_visits', 0))
        except RuntimeError as e:
            logger.warning("Failed to fetch visits for item %s: %s", ext_id, e)

        # Estimate revenue from sold_qty * price
        sold_qty = p.get('sold_qty', 0) or 0
        est_revenue = sold_qty * price

        db = get_db()
        try:
            db.execute('''
                INSERT INTO mp_ads
                    (org_id, platform, external_id, title, listing_type_id, listing_type,
                     visits_30d, price, revenue, conversions, updated_at)
                VALUES (?, 'mercado_livre', ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(org_id, external_id) DO UPDATE SET
                    platform        = 'mercado_livre',
                    title           = excluded.title,
                    listing_type_id = excluded.listing_type_id,
                    listing_type    = excluded.listing_type,
                    visits_30d      = excluded.visits_30d,
                    price           = excluded.price,
                    revenue         = excluded.revenue,
                    conversions     = excluded.conversions,
                    updated_at      = datetime('now')
            ''', (org_id, ext_id, title, listing_type, listing_type,
                  visits_30d, price, est_revenue, sold_qty))
            db.commit()
        finally:
            db.close()

        total_synced += 1

    logger.info("Synced %d promoted ads for org=%s", total_synced, org_id)
    return total_synced


# ── Sub-sync: Competitors ───────────────────────────────────────────────────

def _sync_competitors(org_id, token, user_id):
    """Search ML for competing sellers in same categories."""
    count = 0
    try:
        db = get_db()
        rows = db.execute(
            "SELECT DISTINCT category FROM mp_products WHERE org_id=? AND platform='mercado_livre' AND status='active' AND category != ''",
            (org_id,)
        ).fetchall()
        db.close()

        if not rows:
            logger.info("No categories to search competitors for org=%s", org_id)
            return 0

        our_seller_id = str(user_id)
        seen_sellers = set()

        for row in rows[:3]:  # Limit to top 3 categories
            category = dict(row).get('category', '')
            if not category:
                continue

            try:
                url = f"{ML_API}/sites/MLB/search?category={category}&limit=20&sort=sold_quantity_desc"
                data = api_request(url, headers=_auth(token))
                items = data.get('results', [])

                db = get_db()
                for item in items:
                    seller = item.get('seller', {})
                    seller_id = str(seller.get('id', ''))

                    if not seller_id or seller_id == our_seller_id or seller_id in seen_sellers:
                        continue
                    seen_sellers.add(seller_id)

                    rep = seller.get('seller_reputation', {})
                    trans = rep.get('transactions', {})
                    ratings = trans.get('ratings', {})
                    positive = ratings.get('positive', 0) or 0
                    power = rep.get('power_seller_status') or ''

                    badge_map = {
                        'platinum': 'MercadoLider Platinum',
                        'gold': 'MercadoLider Gold',
                        'silver': 'MercadoLider',
                    }

                    ship = item.get('shipping', {})
                    is_full = 1 if ship.get('logistic_type') == 'fulfillment' else 0
                    is_spons = 1 if item.get('listing_type_id') in ('gold_pro', 'gold_premium') else 0

                    try:
                        db.execute("""
                            INSERT INTO mp_competitors
                                (org_id, platform, seller_id, nickname, rating,
                                 completed_sales, price, stock, badge, fulfillment,
                                 sponsored, sold_qty, power_status, last_synced)
                            VALUES (?, 'mercado_livre', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                            ON CONFLICT(org_id, platform, seller_id) DO UPDATE SET
                                nickname=excluded.nickname, rating=excluded.rating,
                                completed_sales=excluded.completed_sales, price=excluded.price,
                                stock=excluded.stock, badge=excluded.badge,
                                fulfillment=excluded.fulfillment, sponsored=excluded.sponsored,
                                sold_qty=excluded.sold_qty, power_status=excluded.power_status,
                                last_synced=datetime('now')
                        """, (
                            org_id, seller_id, seller.get('nickname', ''),
                            round(positive * 5, 1), trans.get('completed', 0),
                            item.get('price', 0), item.get('available_quantity', 0),
                            badge_map.get(power, power or 'Seller padrao'),
                            is_full, is_spons,
                            item.get('sold_quantity', 0), power,
                        ))
                        count += 1
                    except Exception as e:
                        logger.warning("Error saving competitor %s: %s", seller_id, e)

                db.commit()
                db.close()
            except Exception as e:
                logger.warning("Error searching category %s: %s", category, e)

    except Exception as e:
        logger.error("Error in competitor sync: %s", e)

    logger.info("Synced %d competitors for org=%s", count, org_id)
    return count


# ── Main orchestrator ────────────────────────────────────────────────────────

def sync_all(org_id):
    """Run all Mercado Livre sub-syncs for the given org."""
    token = get_valid_token(org_id, 'mercado_livre')

    from oauth_manager import get_integration
    integration = get_integration(org_id, 'mercado_livre')
    config = integration.get('config', {}) if isinstance(integration.get('config'), dict) else {}
    user_id = config.get('user_id', '') or integration.get('account_id', '')

    if not user_id:
        me = api_request(f'{ML_API}/users/me', headers=_auth(token))
        user_id = str(me.get('id', ''))
        if not user_id:
            raise RuntimeError("Could not determine ML user_id for org %s" % org_id)

    total = 0
    errors = []

    # 1. Account health
    try:
        total += sync_account_health(org_id, token)
    except Exception as e:
        logger.error("Account health sync failed: %s", e)
        errors.append(f"account_health: {e}")

    # 2. Orders
    try:
        total += sync_orders(org_id, token, user_id)
    except Exception as e:
        logger.error("Orders sync failed: %s", e)
        errors.append(f"orders: {e}")

    # 3. Products
    try:
        total += sync_products(org_id, token, user_id)
    except Exception as e:
        logger.error("Products sync failed: %s", e)
        errors.append(f"products: {e}")

    # 4. Returns
    try:
        total += sync_returns(org_id, token, user_id)
    except Exception as e:
        logger.error("Returns sync failed: %s", e)
        errors.append(f"returns: {e}")

    # 5. Ads (promoted listings)
    try:
        total += sync_ads(org_id, token, user_id)
    except Exception as e:
        logger.error("Ads sync failed: %s", e)
        errors.append(f"ads: {e}")

    # 6. Competitors (after products so categories are available)
    try:
        total += _sync_competitors(org_id, token, user_id)
    except Exception as e:
        logger.error("Competitors sync failed: %s", e)
        errors.append(f"competitors: {e}")

    if errors:
        logger.warning("ML sync completed with %d errors: %s",
                        len(errors), '; '.join(errors))

    return total


def run(org_id, max_age=60):
    """Entry point: sync ML data if stale."""
    return run_sync_if_needed(org_id, 'mercado_livre', sync_all,
                              sync_type='full', max_age=max_age)
