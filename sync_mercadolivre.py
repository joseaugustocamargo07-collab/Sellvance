"""
Sellvance - Mercado Livre API Sync
Syncs orders, products, account health, returns from ML API.
"""

import json
from database import get_db
from sync_base import get_valid_token, api_request, AuthError

ML_API = "https://api.mercadolibre.com"


def _auth_headers(token):
    return {"Authorization": f"Bearer {token}"}


def _get_user_id(org_id):
    """Get ML user_id from stored integration config."""
    from oauth_manager import get_integration
    integration = get_integration(org_id, 'mercado_livre')
    if not integration:
        return None
    config = integration.get('config', {})
    return config.get('user_id') or None


def sync_all(org_id):
    """Run all ML sync operations. Returns total records synced."""
    print(f"[ml_sync] Starting sync for org_id={org_id}")
    token = get_valid_token(org_id, 'mercado_livre')
    if not token:
        print(f"[ml_sync] No valid token for org_id={org_id}")
        return 0
    print(f"[ml_sync] Got token: {token[:10]}...")

    total = 0

    # First get user info (also syncs account health)
    user_id = _sync_user_info(org_id, token)
    if not user_id:
        print(f"[ml_sync] Could not get user_id for org_id={org_id}")
        return 0
    print(f"[ml_sync] Got user_id={user_id}")
    total += 1

    # Sync orders
    orders_count = _sync_orders(org_id, token, user_id)
    print(f"[ml_sync] Synced {orders_count} orders")
    total += orders_count

    # Sync products (listings)
    products_count = _sync_products(org_id, token, user_id)
    print(f"[ml_sync] Synced {products_count} products")
    total += products_count

    # Compute returns from orders
    returns_count = _sync_returns(org_id)
    print(f"[ml_sync] Synced returns: {returns_count}")
    total += returns_count

    # Sync competitors via catalog product discovery
    try:
        comp_count = _sync_competitors(org_id, token, user_id)
        print(f"[ml_sync] Synced {comp_count} competitors")
        total += comp_count
    except Exception as e:
        print(f"[ml_sync] Competitor sync error (non-fatal): {e}")

    print(f"[ml_sync] Total records synced: {total}")

    return total


def _sync_user_info(org_id, token):
    """Fetch /users/me and update account health + store user_id."""
    try:
        data = api_request(f"{ML_API}/users/me", _auth_headers(token))
    except AuthError:
        raise
    except Exception as e:
        print(f"[ml_sync] Error fetching user info: {e}")
        return None

    user_id = data.get('id')
    if not user_id:
        return None

    # Save user_id back to integration config if not already saved
    from oauth_manager import get_integration
    integration = get_integration(org_id, 'mercado_livre')
    if integration:
        config = integration.get('config', {})
        if not config.get('user_id'):
            config['user_id'] = str(user_id)
            db = get_db()
            db.execute("UPDATE api_integrations SET config_json=? WHERE org_id=? AND platform='mercado_livre'",
                       (json.dumps(config), org_id))
            db.commit()
            db.close()

    # Extract account health from seller_reputation
    rep = data.get('seller_reputation', {})
    transactions = rep.get('transactions', {})
    ratings = transactions.get('ratings', {})
    metrics = rep.get('metrics', {})
    power_status = rep.get('power_seller_status') or ''

    # Compute health score (0-100)
    positive_pct = ratings.get('positive', 0)
    claims_rate = metrics.get('claims', {}).get('rate', 0)
    delayed_rate = metrics.get('delayed_handling_time', {}).get('rate', 0)
    cancel_rate = metrics.get('cancellations', {}).get('rate', 0)

    # Calculate health score
    if positive_pct > 0:
        base_score = int(positive_pct * 100) if positive_pct <= 1 else int(positive_pct)
    else:
        # New seller with no ratings yet - calculate from metrics
        completed = transactions.get('completed', 0)
        if completed > 0:
            base_score = 85  # Good baseline for active seller
        else:
            base_score = 70  # Neutral for brand new seller

    # Penalize for problems
    base_score -= int(claims_rate * 200)
    base_score -= int(delayed_rate * 150)
    base_score -= int(cancel_rate * 200)
    # Bonus for zero problems
    if claims_rate == 0 and delayed_rate == 0 and cancel_rate == 0 and transactions.get('completed', 0) > 5:
        base_score = max(base_score, 90)
    score = max(0, min(100, base_score))

    level_map = {
        'platinum': 'MercadoLider Platinum',
        'gold': 'MercadoLider Gold',
        'silver': 'MercadoLider',
        '': 'Vendedor Regular'
    }
    level = level_map.get(power_status, power_status or 'Vendedor Regular')

    health_metrics = {
        'reputacao': f"{int(positive_pct*100 if positive_pct<=1 else positive_pct)}%",
        'vendas_completas': str(transactions.get('completed', 0)),
        'reclamacoes': f"{round(claims_rate*100, 1)}%",
        'atrasos': f"{round(delayed_rate*100, 1)}%",
        'cancelamentos': f"{round(cancel_rate*100, 1)}%",
    }

    alerts = []
    if claims_rate > 0.02:
        alerts.append(f"Taxa de reclamacoes alta: {round(claims_rate*100,1)}%")
    if delayed_rate > 0.05:
        alerts.append(f"Atrasos no envio: {round(delayed_rate*100,1)}%")
    if cancel_rate > 0.02:
        alerts.append(f"Taxa de cancelamento alta: {round(cancel_rate*100,1)}%")

    # Upsert account health
    db = get_db()
    db.execute("""
        INSERT INTO mp_account_health (org_id, platform, score, level, metrics_json, alerts_json, last_synced)
        VALUES (?, 'mercado_livre', ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(org_id, platform)
        DO UPDATE SET score=?, level=?, metrics_json=?, alerts_json=?, last_synced=datetime('now')
    """, (org_id, score, level, json.dumps(health_metrics), json.dumps(alerts),
          score, level, json.dumps(health_metrics), json.dumps(alerts)))
    db.commit()
    db.close()

    return str(user_id)


def _sync_orders(org_id, token, user_id):
    """Fetch recent orders from ML and store in orders table."""
    count = 0
    offset = 0
    limit = 50

    while True:
        try:
            url = f"{ML_API}/orders/search?seller={user_id}&sort=date_desc&limit={limit}&offset={offset}"
            data = api_request(url, _auth_headers(token))
        except AuthError:
            raise
        except Exception as e:
            print(f"[ml_sync] Error fetching orders (offset={offset}): {e}")
            break

        results = data.get('results', [])
        if not results:
            break

        db = get_db()
        for order in results:
            ext_id = str(order.get('id', ''))
            if not ext_id:
                continue

            status_map = {
                'paid': 'paid',
                'shipped': 'shipped',
                'delivered': 'delivered',
                'cancelled': 'cancelled',
            }
            status = status_map.get(order.get('status', ''), order.get('status', 'unknown'))
            total = order.get('total_amount', 0) or 0
            shipping_cost = 0
            shipping = order.get('shipping', {})
            if isinstance(shipping, dict):
                shipping_cost = shipping.get('cost', 0) or 0
            revenue = total - shipping_cost
            date_created = order.get('date_created', '')
            if 'T' in date_created:
                date_created = date_created.replace('T', ' ')[:19]

            # Get buyer info
            buyer = order.get('buyer', {})
            buyer_nick = buyer.get('nickname', '')

            try:
                # Update if exists, insert if not
                existing = db.execute(
                    "SELECT id FROM orders WHERE org_id=? AND marketplace='mercado_livre' AND external_id=?",
                    (org_id, ext_id)
                ).fetchone()
                if existing:
                    db.execute("""
                        UPDATE orders SET status=?, gmv=?, revenue=?, ordered_at=?
                        WHERE org_id=? AND marketplace='mercado_livre' AND external_id=?
                    """, (status, total, revenue, date_created, org_id, ext_id))
                else:
                    db.execute("""
                        INSERT INTO orders (org_id, marketplace, external_id, status, gmv, revenue, cost, channel, ordered_at)
                        VALUES (?, 'mercado_livre', ?, ?, ?, ?, 0, 'marketplace', ?)
                    """, (org_id, ext_id, status, total, revenue, date_created))
                count += 1
            except Exception as e:
                print(f"[ml_sync] Order insert error: {e}")

        db.commit()
        db.close()

        total_results = data.get('paging', {}).get('total', 0)
        offset += limit
        if offset >= total_results or offset >= 500:  # Cap at 500 orders for performance
            break

    return count


def _sync_products(org_id, token, user_id):
    """Fetch seller's listings from ML and store in mp_products."""
    count = 0

    try:
        # Get list of item IDs
        url = f"{ML_API}/users/{user_id}/items/search?limit=100"
        data = api_request(url, _auth_headers(token))
        item_ids = data.get('results', [])
    except Exception as e:
        print(f"[ml_sync] Error fetching items list: {e}")
        return 0

    if not item_ids:
        return 0

    # Multi-get items in batches of 20
    db = get_db()
    for i in range(0, len(item_ids), 20):
        batch = item_ids[i:i+20]
        ids_param = ','.join(batch)
        try:
            items_data = api_request(f"{ML_API}/items?ids={ids_param}", _auth_headers(token))
        except Exception as e:
            print(f"[ml_sync] Error fetching items batch: {e}")
            continue

        for item_wrapper in items_data:
            item = item_wrapper.get('body', {})
            if not item or item_wrapper.get('code') != 200:
                continue

            ext_id = item.get('id', '')
            title = item.get('title', '')
            price = item.get('price', 0) or 0
            stock = item.get('available_quantity', 0) or 0
            sold = item.get('sold_quantity', 0) or 0
            thumb = item.get('thumbnail', '')
            listing_type = item.get('listing_type_id', '')
            status = item.get('status', 'active')
            category = item.get('category_id', '')

            try:
                db.execute("""
                    INSERT INTO mp_products (org_id, platform, external_id, title, price, stock_qty, sold_qty, thumbnail_url, listing_type, category, status, last_synced)
                    VALUES (?, 'mercado_livre', ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(org_id, platform, external_id)
                    DO UPDATE SET title=?, price=?, stock_qty=?, sold_qty=?, thumbnail_url=?, listing_type=?, status=?, last_synced=datetime('now')
                """, (org_id, ext_id, title, price, stock, sold, thumb, listing_type, category, status,
                      title, price, stock, sold, thumb, listing_type, status))
                count += 1
            except Exception:
                pass

    db.commit()
    db.close()

    # Also update stock_items from products
    _update_stock_from_products(org_id)

    return count


def _update_stock_from_products(org_id):
    """Update stock_items table from synced mp_products data."""
    db = get_db()
    products = db.execute(
        "SELECT * FROM mp_products WHERE org_id=? AND platform='mercado_livre' AND status='active'",
        (org_id,)
    ).fetchall()

    for p in products:
        sku = p['external_id']
        existing = db.execute("SELECT id FROM stock_items WHERE org_id=? AND sku=?", (org_id, sku)).fetchone()
        stock = p['stock_qty'] or 0
        price = p['price'] or 0
        name = p['title'] or ''

        if stock <= 0:
            status = 'out'
        elif stock < 10:
            status = 'critical'
        elif stock < 20:
            status = 'low'
        else:
            status = 'ok'

        if existing:
            db.execute("""
                UPDATE stock_items SET name=?, stock_qty=?, sale_price=?, status=?, last_updated=datetime('now')
                WHERE org_id=? AND sku=?
            """, (name, stock, price, status, org_id, sku))
        else:
            db.execute("""
                INSERT INTO stock_items (org_id, sku, name, marketplace, stock_qty, sale_price, status, last_updated)
                VALUES (?, ?, ?, 'mercado_livre', ?, ?, ?, datetime('now'))
            """, (org_id, sku, name, stock, price, status))

    db.commit()
    db.close()


def _sync_returns(org_id):
    """Compute returns data from synced orders."""
    db = get_db()

    total = db.execute(
        "SELECT COUNT(*) as cnt FROM orders WHERE org_id=? AND marketplace='mercado_livre'", (org_id,)
    ).fetchone()['cnt']

    returned = db.execute(
        "SELECT COUNT(*) as cnt FROM orders WHERE org_id=? AND marketplace='mercado_livre' AND status IN ('cancelled','returned')",
        (org_id,)
    ).fetchone()['cnt']

    if total == 0:
        db.close()
        return 0

    rate = round(returned / total * 100, 1)
    refunded = db.execute(
        "SELECT COALESCE(SUM(revenue), 0) as total FROM orders WHERE org_id=? AND marketplace='mercado_livre' AND status IN ('cancelled','returned')",
        (org_id,)
    ).fetchone()['total']

    reasons = json.dumps([
        {'reason': 'Pedido cancelado', 'count': returned, 'pct': 100}
    ])

    db.execute("""
        INSERT INTO mp_returns (org_id, platform, total_orders, total_returns, return_rate, reasons_json, refunded_revenue, trend, last_synced)
        VALUES (?, 'mercado_livre', ?, ?, ?, ?, ?, 'stable', datetime('now'))
        ON CONFLICT(org_id, platform)
        DO UPDATE SET total_orders=?, total_returns=?, return_rate=?, reasons_json=?, refunded_revenue=?, last_synced=datetime('now')
    """, (org_id, total, returned, rate, reasons, refunded,
          total, returned, rate, reasons, refunded))
    db.commit()
    db.close()
    return 1

def _sync_competitors(org_id, token, user_id):
    """Discover competitors using catalog products and item multi-get.

    Strategy (all endpoints work from cloud servers):
    1. Get our items via multi-get /items?ids=... (includes catalog_product_id)
    2. For catalog items: GET /products/{catalog_product_id}/items to find other sellers
    3. For found competitor items: multi-get /items?ids=... to get seller details
    4. Fallback: GET /users/{seller_id} for any seller we found
    """
    count = 0
    # Ensure table exists
    try:
        _db = get_db()
        _db.execute("""CREATE TABLE IF NOT EXISTS mp_competitors (
            id INTEGER PRIMARY KEY AUTOINCREMENT, org_id INTEGER NOT NULL,
            platform TEXT NOT NULL DEFAULT 'mercado_livre', seller_id TEXT NOT NULL,
            nickname TEXT DEFAULT '', rating REAL DEFAULT 0, completed_sales INTEGER DEFAULT 0,
            price REAL DEFAULT 0, stock INTEGER DEFAULT 0, badge TEXT DEFAULT '',
            fulfillment INTEGER DEFAULT 0, sponsored INTEGER DEFAULT 0, sold_qty INTEGER DEFAULT 0,
            power_status TEXT DEFAULT '', last_synced TEXT DEFAULT (datetime('now')),
            UNIQUE(org_id, platform, seller_id))""")
        _db.commit()
        _db.close()
    except Exception:
        pass
    try:
        db = get_db()
        rows = db.execute(
            "SELECT external_id, category FROM mp_products WHERE org_id=? AND platform='mercado_livre' AND status='active'",
            (org_id,)
        ).fetchall()
        db.close()

        if not rows:
            print("[ml_sync] No active products for competitor discovery")
            return 0

        our_seller_id = str(user_id)
        seen_sellers = set()
        competitor_item_ids = []
        headers = _auth(token)

        item_ids = [dict(r)['external_id'] for r in rows if dict(r).get('external_id')]
        print(f"[ml_sync] Checking {len(item_ids)} items for catalog products...")

        # Step 1: Multi-get our items to find catalog_product_id
        catalog_products = []
        for batch_start in range(0, len(item_ids), 20):
            batch = item_ids[batch_start:batch_start+20]
            ids_str = ','.join(batch)
            try:
                multi_url = f"{ML_API}/items?ids={ids_str}&attributes=id,catalog_product_id,category_id,seller_id"
                multi_resp = api_request(multi_url, headers)
                for entry in multi_resp:
                    if entry.get('code') != 200:
                        continue
                    item = entry.get('body', {})
                    cat_prod_id = item.get('catalog_product_id')
                    if cat_prod_id:
                        catalog_products.append(cat_prod_id)
                        print(f"[ml_sync] Item {item.get('id')} -> catalog: {cat_prod_id}")
            except Exception as e:
                print(f"[ml_sync] Multi-get failed: {e}")

        print(f"[ml_sync] Found {len(catalog_products)} catalog products")

        # Step 2: For each catalog product, find other sellers' items
        for cat_prod_id in catalog_products[:5]:  # Limit to 5 catalog products
            try:
                # This endpoint returns items from different sellers for same catalog product
                cat_url = f"{ML_API}/products/{cat_prod_id}/items?status=active&limit=20"
                cat_resp = api_request(cat_url, headers)

                # Response can be a list of item IDs or objects
                items = cat_resp if isinstance(cat_resp, list) else cat_resp.get('results', cat_resp.get('items', []))
                print(f"[ml_sync] Catalog {cat_prod_id}: {len(items)} items found")

                for ci in items:
                    ci_id = ci if isinstance(ci, str) else ci.get('id', ci.get('item_id', ''))
                    if ci_id and str(ci_id) not in item_ids:
                        competitor_item_ids.append(str(ci_id))
            except Exception as e:
                print(f"[ml_sync] Catalog {cat_prod_id} failed: {e}")

        # Step 3: If no catalog items found, try category-based approach
        # Use /sites/MLB/search with access_token (may work for some categories)
        if not competitor_item_ids:
            print("[ml_sync] No catalog items, trying category search...")
            categories = list(set(dict(r).get('category', '') for r in rows if dict(r).get('category')))
            for cat_id in categories[:2]:
                if not cat_id:
                    continue
                try:
                    import urllib.request as _ur
                    search_url = f"{ML_API}/sites/MLB/search?category={cat_id}&limit=20&sort=sold_quantity_desc&access_token={token}"
                    _req = _ur.Request(search_url, headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept': 'application/json',
                    })
                    _raw = _ur.urlopen(_req, timeout=15).read()
                    resp = json.loads(_raw)
                    items = resp.get('results', [])
                    if items:
                        print(f"[ml_sync] Category search for {cat_id}: {len(items)} results")
                        for it in items:
                            if isinstance(it, dict):
                                it_id = it.get('id', '')
                                if it_id and str(it_id) not in item_ids:
                                    competitor_item_ids.append(str(it_id))
                            elif isinstance(it, str) and it not in item_ids:
                                competitor_item_ids.append(it)
                        if competitor_item_ids:
                            break
                except Exception as e:
                    print(f"[ml_sync] Category search {cat_id} failed: {e}")

        if not competitor_item_ids:
            print("[ml_sync] No competitor items found via any method")
            return 0

        # Deduplicate
        competitor_item_ids = list(dict.fromkeys(competitor_item_ids))[:50]
        print(f"[ml_sync] Fetching details for {len(competitor_item_ids)} competitor items...")

        # Step 4: Multi-get competitor items for full details
        items_to_save = []
        for batch_start in range(0, len(competitor_item_ids), 20):
            batch = competitor_item_ids[batch_start:batch_start+20]
            ids_str = ','.join(batch)
            try:
                multi_url = f"{ML_API}/items?ids={ids_str}"
                multi_resp = api_request(multi_url, headers)
                for entry in multi_resp:
                    if entry.get('code') == 200:
                        items_to_save.append(entry.get('body', {}))
            except Exception as e:
                print(f"[ml_sync] Batch fetch failed: {e}")

        # Step 5: Save competitors to DB
        db = get_db()
        for item in items_to_save:
            seller = item.get('seller', {}) or {}
            seller_id = str(seller.get('id', ''))

            if not seller_id or seller_id == our_seller_id or seller_id in seen_sellers:
                continue
            seen_sellers.add(seller_id)

            rep = seller.get('seller_reputation', {}) or {}
            trans = rep.get('transactions', {}) or {}
            ratings = trans.get('ratings', {}) or {}
            positive = ratings.get('positive', 0) or 0
            power = rep.get('power_seller_status') or ''

            badge_map = {
                'platinum': 'MercadoLider Platinum',
                'gold': 'MercadoLider Gold',
                'silver': 'MercadoLider',
            }

            ship = item.get('shipping', {}) or {}
            is_full = 1 if ship.get('logistic_type') == 'fulfillment' else 0
            is_spons = 1 if item.get('listing_type_id') in ('gold_pro', 'gold_premium') else 0

            try:
                db.execute("""
                    INSERT INTO mp_competitors (org_id, platform, seller_id, nickname, rating,
                        completed_sales, price, stock, badge, fulfillment, sponsored,
                        sold_qty, power_status, last_synced)
                    VALUES (?, 'mercado_livre', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(org_id, platform, seller_id)
                    DO UPDATE SET nickname=?, rating=?, completed_sales=?, price=?,
                        stock=?, badge=?, fulfillment=?, sponsored=?, sold_qty=?,
                        power_status=?, last_synced=datetime('now')
                """, (
                    org_id, seller_id, seller.get('nickname', ''),
                    round(positive * 5, 1), trans.get('completed', 0),
                    item.get('price', 0), item.get('available_quantity', 0),
                    badge_map.get(power, power or 'Seller padrao'),
                    is_full, is_spons,
                    item.get('sold_quantity', 0), power,
                    seller.get('nickname', ''), round(positive * 5, 1),
                    trans.get('completed', 0), item.get('price', 0),
                    item.get('available_quantity', 0),
                    badge_map.get(power, power or 'Seller padrao'),
                    is_full, is_spons,
                    item.get('sold_quantity', 0), power,
                ))
                count += 1
            except Exception as e:
                print(f"[ml_sync] Error saving competitor {seller_id}: {e}")

        db.commit()
        db.close()
        print(f"[ml_sync] Saved {count} competitors")

    except AuthError:
        raise
    except Exception as e:
        print(f"[ml_sync] Error in competitor sync: {e}")
        import traceback
        traceback.print_exc()

    return count

