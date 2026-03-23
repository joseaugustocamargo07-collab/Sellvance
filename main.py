# Sellvance App — main server
from flask import Flask, render_template, request, session, redirect, url_for, jsonify, send_file
from database import get_db
from auth import login_required, verify_password, hash_password
from traffic_ai import analyze_all, calc_metrics, score_campaign
import os
import json

app = Flask(__name__, template_folder='.')

# SECRET_KEY obrigatorio via variavel de ambiente (sem fallback)
_secret = os.environ.get('SECRET_KEY')
if not _secret:
    raise RuntimeError('[SELLVANCE] SECRET_KEY nao configurada. Defina a variavel de ambiente no Railway.')
app.secret_key = _secret
app.config['PERMANENT_SESSION_LIFETIME'] = 86400  # sessao expira em 24h

# Rate limiting — protege contra brute force
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    _limiter = Limiter(app=app, key_func=get_remote_address,
                       default_limits=[], storage_uri='memory://')
    _limiter_ok = True
except ImportError:
    _limiter_ok = False
    _limiter = None

_db_ready = False

@app.before_request
def ensure_db_ready():
    global _db_ready
    if not _db_ready:
        _db_ready = True
        try:
            import os as _os
            from database import init_db, migrate_db, DB_PATH, DATA_DIR
            # Update org name from connected ML account
            try:
                from database import get_db as _gdb
                _d = _gdb()
                _ml = _d.execute("SELECT account_name FROM integrations WHERE platform='mercado_livre' AND status='connected' AND account_name IS NOT NULL LIMIT 1").fetchone()
                if _ml and dict(_ml).get('account_name'):
                    _d.execute("UPDATE organizations SET name=? WHERE id=1", (dict(_ml)['account_name'],))
                    _d.commit()
                _d.close()
            except Exception:
                pass
            print(f"[startup] DB_PATH={DB_PATH}")
            print(f"[startup] DATA_DIR={DATA_DIR}")
            print(f"[startup] RAILWAY_VOLUME={_os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', 'NOT SET')}")
            print(f"[startup] DB exists={_os.path.exists(DB_PATH)}")
            if _os.path.exists(DB_PATH):
                print(f"[startup] DB size={_os.path.getsize(DB_PATH)} bytes")
            init_db()
            migrate_db()
        except Exception as e:
            import traceback
            traceback.print_exc()

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'app': 'Sellvance CRM'}), 200

@app.route('/')
@app.route('/dashboard')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    db = get_db()
    org_id = session.get('org_id', 1)

    # KPIs from orders
    kpis_raw = db.execute('''
        SELECT COALESCE(SUM(revenue), 0) as total_revenue,
               COUNT(*) as total_orders,
               COUNT(DISTINCT contact_id) as unique_customers
        FROM orders WHERE org_id = ?
    ''', (org_id,)).fetchone()
    kpis = dict(kpis_raw)

    # ROAS from ad_campaigns
    ads = db.execute('''
        SELECT COALESCE(SUM(spend), 0) as total_spend,
               COALESCE(SUM(revenue), 0) as total_revenue
        FROM ad_campaigns WHERE org_id = ?
    ''', (org_id,)).fetchone()
    total_spend = ads['total_spend'] or 1
    roas = round(ads['total_revenue'] / total_spend, 2) if total_spend > 0 else 0

    # CAC
    new_customers = db.execute(
        'SELECT COUNT(*) as cnt FROM contacts WHERE org_id = ?', (org_id,)
    ).fetchone()['cnt'] or 1
    cac = round(total_spend / max(new_customers, 1), 2)

    # Channel performance
    channel_perf = db.execute('''
        SELECT channel, COALESCE(SUM(revenue), 0) as revenue, COUNT(*) as orders
        FROM orders WHERE org_id = ?
        GROUP BY channel ORDER BY revenue DESC
    ''', (org_id,)).fetchall()

    return render_template('dashboard.html', kpis=kpis, roas=roas, cac=cac, channel_perf=channel_perf)

@_limiter.limit('5 per minute')
@app.route('/login', methods=['GET', 'POST'])
def login():
    # Rate limit: 5 tentativas por minuto por IP (via @_limiter.limit)
    if request.method == 'POST':
        email    = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        if not email or not password:
            return render_template('login.html', error='Preencha email e senha')
        db   = get_db()
        user = db.execute('SELECT * FROM users WHERE LOWER(email) = ?', (email,)).fetchone()
        db.close()
        if user and verify_password(password, user['password_hash']):
            session.permanent = True
            session['user_id']   = user['id']
            session['user_name'] = user['name']
            session['org_id']    = user['org_id']
            session['org_name']  = user['org_name']
            return redirect(url_for('dashboard'))
        return render_template('login.html', error='Email ou senha invalidos')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/crm')
@login_required
def crm():
    db     = get_db()
    org_id = session.get('org_id', 1)
    # Aggregate stats via SQL (efficient — no full-table load)
    stats = db.execute('''
        SELECT COUNT(*) as total_contacts,
               COALESCE(SUM(ltv), 0) as total_ltv,
               SUM(CASE WHEN total_orders > 1 THEN 1 ELSE 0 END) as repeat_buyers
        FROM contacts WHERE org_id = ?
    ''', (org_id,)).fetchone()
    total_contacts = stats['total_contacts'] or 0
    total_ltv      = stats['total_ltv']      or 0
    repeat_buyers  = stats['repeat_buyers']  or 0
    recompra_rate  = round(repeat_buyers / max(total_contacts, 1) * 100, 1)
    # RFM distribution
    rfm_rows = db.execute('''
        SELECT rfm_segment, COUNT(*) as cnt
        FROM contacts WHERE org_id = ? GROUP BY rfm_segment
    ''', (org_id,)).fetchall()
    rfm = {r['rfm_segment']: r['cnt'] for r in rfm_rows}
    search   = request.args.get('search', '').strip()
    page     = request.args.get('page', 1, type=int)
    per_page = 50
    # With search: re-count filtered results
    if search:
        count_row = db.execute(
            '''SELECT COUNT(*) as cnt FROM contacts
               WHERE org_id=? AND (name LIKE ? OR email LIKE ? OR phone LIKE ?)''',
            (org_id, f'%{search}%', f'%{search}%', f'%{search}%')).fetchone()
        total_contacts = count_row['cnt'] or 0
    total_pages = max(1, (total_contacts + per_page - 1) // per_page)
    page        = max(1, min(page, total_pages))
    offset      = (page - 1) * per_page
    # Fetch only the current page — SQL LIMIT/OFFSET
    if search:
        paged = db.execute(
            '''SELECT * FROM contacts
               WHERE org_id=? AND (name LIKE ? OR email LIKE ? OR phone LIKE ?)
               ORDER BY ltv DESC LIMIT ? OFFSET ?''',
            (org_id, f'%{search}%', f'%{search}%', f'%{search}%',
             per_page, offset)).fetchall()
    else:
        paged = db.execute(
            '''SELECT * FROM contacts WHERE org_id=?
               ORDER BY ltv DESC LIMIT ? OFFSET ?''',
            (org_id, per_page, offset)).fetchall()
    db.close()
    return render_template('crm.html', contacts=paged, total_contacts=total_contacts,
                           total_ltv=total_ltv, recompra_rate=recompra_rate, rfm=rfm,
                           search=search, cur_page=page, total_pages=total_pages)

@app.route('/ranking')
@login_required
def ranking():
    import re as _re
    db     = get_db()
    org_id = session.get('org_id', 1)

    # Check which platforms are connected (have a valid access_token)
    integrations = db.execute(
        "SELECT platform, status FROM api_integrations WHERE org_id=?", (org_id,)
    ).fetchall()
    connected_platforms = {r['platform'] for r in integrations if r['status'] == 'connected'}
    meta_connected   = 'meta_ads'   in connected_platforms
    google_connected = 'google_ads' in connected_platforms
    tiktok_connected = 'tiktok_ads' in connected_platforms

    # Demo external_campaign_id pattern: meta_N, goog_N, tik_N (seeded at init)
    _DEMO_RE = _re.compile(r'^(meta|goog|tik)_\d+$')

    all_camps = db.execute('SELECT * FROM ad_campaigns WHERE org_id = ?', (org_id,)).fetchall()
    real_camps = [c for c in all_camps if not _DEMO_RE.match(dict(c).get('external_campaign_id') or '')]
    has_real_data = len(real_camps) > 0

    campaigns_raw = real_camps if has_real_data else all_camps

    # Last sync time for Meta
    last_sync_meta = None
    try:
        from sync_base import get_last_sync_info
        info = get_last_sync_info(org_id, 'meta_ads')
        last_sync_meta = info.get('finished_at') if info else None
    except Exception:
        pass

    campaigns      = []
    revenue_wasted = 0
    for c in campaigns_raw:
        c_dict = dict(c)
        m = calc_metrics(c_dict)
        s = score_campaign(c_dict, m)
        if s['score'] >= 75:
            action = 'scale'
        elif s['score'] >= 50:
            action = 'optimize'
        else:
            action = 'pause'
            revenue_wasted += c_dict.get('spend', 0)
        campaigns.append({**c_dict, **m, **s, 'action': action})
    campaigns.sort(key=lambda x: x['score'], reverse=True)
    return render_template('ranking.html',
                           campaigns=campaigns,
                           revenue_wasted=revenue_wasted,
                           has_real_data=has_real_data,
                           meta_connected=meta_connected,
                           google_connected=google_connected,
                           tiktok_connected=tiktok_connected,
                           last_sync_meta=last_sync_meta)

@app.route('/marketplaces')
@login_required
def marketplaces():
    try:
        return _marketplaces_inner()
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

def _marketplaces_inner():
    from marketplace_strategy import compute_marketplace_scores, get_rebid_recommendations
    from marketplace_intel import (COMPETITORS, MY_PRODUCTS, MP_ADS_DATA, RETURNS_DATA,
                                   ACCOUNT_HEALTH, analyze_competitive_position,
                                   analyze_mp_ads, get_keyword_opportunities,
                                   get_my_products_live, get_account_health_live,
                                   get_returns_live, get_mp_totals_live, is_platform_synced,
                                   get_ads_live, get_real_orders_totals, get_real_products_list, get_ads_from_campaigns,
                                   get_keywords_from_products, search_ml_competitors, compute_health_score)
    from oauth_manager import get_integration
    from sync_base import run_sync_if_needed, get_last_sync_info
    mp  = request.args.get('mp', 'mercado_livre')
    tab = request.args.get('tab', 'overview')
    date_start = request.args.get('date_start', '')
    date_end = request.args.get('date_end', '')
    org_id = session.get('org_id', 1)
    # All known platforms catalog
    _MP_CATALOG = {
        'mercado_livre': {'name': 'Mercado Livre', 'icon': '\U0001f6d2', 'color': '#ffe600'},
        'amazon':        {'name': 'Amazon',        'icon': '\U0001f4e6', 'color': '#ff9900'},
        'tiktok_shop':   {'name': 'TikTok Shop',   'icon': '\U0001f3b5', 'color': '#ff0050'},
        'shopee':        {'name': 'Shopee',         'icon': '\U0001f9e1', 'color': '#ee4d2d'},
    }
    # Default order (connected ones will always show, disconnected hidden unless already selected)
    _default_order = ['mercado_livre', 'amazon', 'tiktok_shop', 'shopee']
    # Get all integrations for this org
    from oauth_manager import get_all_integrations as _get_all_integ
    _all_integ = _get_all_integ(org_id)  # dict keyed by platform
    all_mp = []
    for _pid in _default_order:
        _info = _MP_CATALOG.get(_pid, {})
        _conn = _all_integ.get(_pid, {})
        _is_connected = _conn.get('status') == 'connected'
        # Include platform if: it's one of the main 3 OR it's connected
        if _pid in ('mercado_livre', 'amazon', 'tiktok_shop') or _is_connected:
            all_mp.append({
                'id': _pid,
                'name': _info['name'],
                'icon': _info['icon'],
                'color': _info['color'],
                'is_connected': _is_connected,
                'account_name': _conn.get('account_name', ''),
            })
    # Also add any connected platform not in default list
    for _pid, _conn in _all_integ.items():
        if _conn.get('status') == 'connected' and _pid not in _default_order:
            _info = _MP_CATALOG.get(_pid, {'name': _pid.title(), 'icon': '\U0001f6cd', 'color': '#6b7280'})
            all_mp.append({
                'id': _pid, 'name': _info['name'], 'icon': _info['icon'],
                'color': _info['color'], 'is_connected': True,
                'account_name': _conn.get('account_name', ''),
            })
    # Check if platform is connected and trigger sync if needed
    integration = get_integration(org_id, mp)
    is_connected = integration and integration.get('status') == 'connected'
    sync_info = None

    if is_connected and mp == 'mercado_livre':
        from sync_mercadolivre import sync_all as ml_sync
        run_sync_if_needed(org_id, mp, ml_sync, max_age=60)
        sync_info = get_last_sync_info(org_id, mp)

    if is_connected and is_platform_synced(org_id, mp):
        # ── REAL DATA MODE ──────────────────────────────────
        health      = get_account_health_live(org_id, mp)
        my_product  = get_my_products_live(org_id, mp)
        returns     = get_returns_live(org_id, mp)

        # Search for real competitors on ML
        competitors = search_ml_competitors(org_id, mp)

        # Generate keywords from actual product titles
        keywords = get_keywords_from_products(org_id, mp)
        if not keywords:
            keywords = get_keyword_opportunities(mp)

        # Compute real health score from metrics
        if health.get('score', 0) == 0 and health.get('metrics'):
            health['score'] = compute_health_score(health['metrics'])

        # Build comp_analysis using COMPETITOR prices for range, our price for position
        real_products = get_real_products_list(org_id, mp)
        my_product['_live'] = True
        if real_products:
            my_product['_products'] = real_products

        # Use competitor prices for meaningful range/avg
        comp_prices = [c.get('price_32l', 0) for c in competitors if c.get('price_32l', 0) > 0]
        my_p = my_product.get('price_32l', 0)

        # Include our price in the range too
        all_prices = comp_prices + ([my_p] if my_p > 0 else [])

        if all_prices:
            max_p = round(max(all_prices), 2)
            min_p = round(min(all_prices), 2)
            avg_p = round(sum(comp_prices) / len(comp_prices), 2) if comp_prices else my_p
            if avg_p > 0 and my_p > 0:
                if my_p > avg_p * 1.1:
                    pos = 'acima'
                elif my_p < avg_p * 0.9:
                    pos = 'abaixo'
                else:
                    pos = 'na_media'
            else:
                pos = 'na_media'
            analysis = {
                'max_price_32l': max_p,
                'min_price_32l': min_p,
                'avg_price_32l': avg_p,
                'price_position': pos,
                'opportunities': [],
            }
        else:
            analysis = analyze_competitive_position(mp)

        # Real ads from synced data (template expects a LIST of ad dicts)
        ads = []  # Default: empty list = no ads
        ads_data = get_ads_live(org_id, mp)
        if ads_data and ads_data.get('_live'):
            if ads_data.get('_from_products'):
                # Promoted products from mp_products (no spend data from ML API)
                for p in ads_data.get('ads', []):
                    revenue = p.get('_revenue_estimated', 0)
                    listing_label = p.get('_listing_label', 'Promovido')
                    ads.append({
                        'name': p.get('title', 'Anúncio'),
                        'type': listing_label,
                        'spend': 0,
                        'revenue': revenue,
                        'acos': 0,
                        'roas': 0,
                        'ctr': 0,
                        'conversions': p.get('sold_qty', 0) or 0,
                        'action_label': 'Monitorar',
                        'price': p.get('price', 0),
                        'stock': p.get('stock_qty', 0),
                        'status': p.get('status', ''),
                        'thumbnail': p.get('thumbnail_url', ''),
                    })
            elif ads_data.get('ads'):
                # Direct mp_ads data (has spend/revenue)
                for a in ads_data['ads']:
                    spend = a.get('spend', 0) or 0
                    rev = a.get('revenue', 0) or 0
                    ads.append({
                        'name': a.get('title', a.get('name', 'Anúncio')),
                        'type': a.get('listing_type_id', 'promoted'),
                        'spend': spend,
                        'revenue': rev,
                        'acos': round(spend / rev * 100, 1) if rev > 0 else 0,
                        'roas': round(rev / spend, 1) if spend > 0 else 0,
                        'ctr': round(a.get('ctr', 0) or 0, 2),
                        'conversions': a.get('conversions', 0) or 0,
                        'action_label': 'Monitorar',
                    })
        if not ads:
            # Check ad_campaigns table
            real_campaigns = get_ads_from_campaigns(org_id, mp)
            for c in real_campaigns:
                spend = c.get('spend', 0) or 0
                rev = c.get('revenue', 0) or 0
                ads.append({
                    'name': c.get('name', 'Campanha'),
                    'type': c.get('objective', 'campaign'),
                    'spend': spend,
                    'revenue': rev,
                    'acos': round(spend / rev * 100, 1) if rev > 0 else 0,
                    'roas': round(rev / spend, 1) if spend > 0 else 0,
                    'ctr': round(c.get('clicks', 0) / max(c.get('impressions', 1), 1) * 100, 2),
                    'conversions': c.get('conversions', 0) or 0,
                    'action_label': 'Monitorar',
                })

        is_live = True
    else:
        # ── DEMO DATA MODE ──────────────────────────────────
        health      = ACCOUNT_HEALTH.get(mp, {'score': 0, 'metrics': {}, 'alerts': []})
        my_product  = MY_PRODUCTS.get(mp, {})
        competitors = COMPETITORS.get(mp, [])
        analysis    = analyze_competitive_position(mp)
        ads         = analyze_mp_ads(mp)
        returns     = RETURNS_DATA.get(mp, {})
        keywords    = get_keyword_opportunities(mp)
        is_live     = False

    db          = get_db()
    stock_items = db.execute('SELECT * FROM stock_items WHERE org_id = ? AND marketplace = ?',
                             (org_id, mp)).fetchall()

    # Aggregate marketplace totals from orders (with date filter)
    mp_totals = {}
    for m_id in ['mercado_livre', 'amazon', 'tiktok_shop']:
        # Check if this marketplace is connected - if so, only count real orders
        m_integration = get_integration(org_id, m_id)
        m_connected = m_integration and m_integration.get('status') == 'connected'

        if m_connected and is_platform_synced(org_id, m_id):
            # Only real orders (with external_id from sync)
            mp_totals[m_id] = get_real_orders_totals(org_id, m_id, date_start, date_end)
        else:
            # Demo mode - use all orders
            mp_sql = 'SELECT COALESCE(SUM(revenue), 0) as revenue, COUNT(*) as orders FROM orders WHERE org_id = ? AND marketplace = ?'
            mp_params = [org_id, m_id]
            if date_start:
                mp_sql += " AND date(ordered_at) >= date(?)"
                mp_params.append(date_start)
            if date_end:
                mp_sql += " AND date(ordered_at) <= date(?)"
                mp_params.append(date_end)
            row = db.execute(mp_sql, mp_params).fetchone()
            mp_totals[m_id] = {'revenue': row['revenue'], 'orders': row['orders']}
    db.close()

    account_name = integration.get('account_name', '') if integration else ''

    # Strategy scores & re-bid recommendations
    try:
        strategy_scores = compute_marketplace_scores(org_id)
        rebid_recs = get_rebid_recommendations(org_id, mp)
    except Exception:
        strategy_scores = []
        rebid_recs = []

    # Always override comp_analysis with REAL DB prices (never demo data)
    try:
        _db2 = get_db()
        _my_row = _db2.execute(
            "SELECT AVG(price) as avg_p FROM mp_products WHERE org_id=? AND platform=? AND price > 0",
            (org_id, mp)
        ).fetchone()
        _cp_row = _db2.execute(
            "SELECT AVG(price) as avg_p, MIN(price) as min_p, MAX(price) as max_p FROM mp_competitors WHERE org_id=? AND platform=? AND price > 0",
            (org_id, mp)
        ).fetchone()
        _db2.close()
        _my_p = round(float(_my_row['avg_p'] or 0), 2) if _my_row and _my_row['avg_p'] else 0
        _avg_p = round(float(_cp_row['avg_p'] or 0), 2) if _cp_row and _cp_row['avg_p'] else 0
        _min_p = round(float(_cp_row['min_p'] or 0), 2) if _cp_row and _cp_row['min_p'] else 0
        _max_p = round(float(_cp_row['max_p'] or 0), 2) if _cp_row and _cp_row['max_p'] else 0
        if _my_p > 0:
            _use_avg = _avg_p if _avg_p > 0 else _my_p
            _pos = 'acima' if _my_p > _use_avg * 1.1 else 'abaixo' if _my_p < _use_avg * 0.9 else 'na_media'
            analysis = {
                'avg_price_32l': _use_avg,
                'min_price_32l': _min_p,
                'max_price_32l': _max_p,
                'avg_price': _use_avg,
                'my_price': _my_p,
                'price_position': _pos,
                'opportunities': analysis.get('opportunities', []) if isinstance(analysis, dict) else [],
            }
    except Exception as _e:
        print(f"[comp_analysis override] {_e}")

    return render_template('traffic.html', mp=mp, tab=tab, all_mp=all_mp, health=health, is_live=is_live, sync_info=sync_info, account_name=account_name,
                           competitors=competitors, my=my_product, comp_analysis=analysis,
                           ads=ads, returns=returns, keywords=keywords, stock_items=stock_items,
                           mp_totals=mp_totals, date_start=date_start, date_end=date_end,
                           strategy_scores=strategy_scores, rebid_recs=rebid_recs)






@app.route('/api/save-competitors', methods=['POST'])
def save_competitors():
    """Receive competitor data from browser-side ML search."""
    try:
        data = request.get_json()
        items = data.get('items', [])
        org_id = data.get('org_id', 1)
        our_seller_id = str(data.get('our_seller_id', ''))
        if not items:
            return jsonify({'status': 'error', 'error': 'No items'})

        from database import get_db
        db = get_db()
        count = 0
        seen = set()
        for item in items:
            seller = item.get('seller', {}) or {}
            sid = str(seller.get('id', ''))
            if not sid or sid == our_seller_id or sid in seen:
                continue
            seen.add(sid)
            rep = seller.get('seller_reputation', {}) or {}
            trans = rep.get('transactions', {}) or {}
            ratings = trans.get('ratings', {}) or {}
            positive = ratings.get('positive', 0) or 0
            power = rep.get('power_seller_status') or ''
            badge_map = {'platinum':'MercadoLider Platinum','gold':'MercadoLider Gold','silver':'MercadoLider'}
            ship = item.get('shipping', {}) or {}
            is_full = 1 if ship.get('logistic_type') == 'fulfillment' else 0
            is_spons = 1 if item.get('listing_type_id') in ('gold_pro','gold_premium') else 0
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
                    org_id, sid, seller.get('nickname',''),
                    round(positive*5,1), trans.get('completed',0),
                    item.get('price',0), item.get('available_quantity',0),
                    badge_map.get(power, power or 'Seller padrao'),
                    is_full, is_spons, item.get('sold_quantity',0), power,
                    seller.get('nickname',''), round(positive*5,1),
                    trans.get('completed',0), item.get('price',0),
                    item.get('available_quantity',0),
                    badge_map.get(power, power or 'Seller padrao'),
                    is_full, is_spons, item.get('sold_quantity',0), power,
                ))
                count += 1
            except Exception:
                pass
        db.commit()
        db.close()
        return jsonify({'status': 'ok', 'saved': count})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)})




@app.route('/api/add-competitor', methods=['POST'])
@login_required
def add_competitor():
    """Manually add a competitor by ML seller ID."""
    try:
        data = request.get_json() or {}
        seller_input = data.get('seller_id', '').strip()

        if not seller_input:
            return jsonify({'status': 'error', 'error': 'Informe o ID do vendedor'})

        import re
        seller_id = re.sub(r'[^0-9]', '', seller_input)
        if not seller_id:
            return jsonify({'status': 'error', 'error': 'ID do vendedor deve ser numerico'})

        org_id = session.get('org_id', 1)
        from database import get_db
        db = get_db()
        integ = db.execute(
            "SELECT access_token FROM integrations WHERE org_id=? AND platform='mercado_livre' AND status='connected'",
            (org_id,)
        ).fetchone()

        if not integ:
            db.close()
            return jsonify({'status': 'error', 'error': 'Conecte sua conta ML primeiro'})

        token = dict(integ)['access_token']

        # Fetch seller info from ML API
        import urllib.request as ur
        headers_ml = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

        try:
            req_ml = ur.Request(f'https://api.mercadolibre.com/users/{seller_id}', headers=headers_ml)
            seller_data = json.loads(ur.urlopen(req_ml, timeout=10).read())
        except Exception as e:
            db.close()
            return jsonify({'status': 'error', 'error': f'Vendedor nao encontrado: {str(e)}'})

        nickname = seller_data.get('nickname', '')
        rep = seller_data.get('seller_reputation', {}) or {}
        trans = rep.get('transactions', {}) or {}
        ratings = trans.get('ratings', {}) or {}
        positive = ratings.get('positive', 0) or 0
        power = rep.get('power_seller_status') or ''

        badge_map = {
            'platinum': 'MercadoLider Platinum',
            'gold': 'MercadoLider Gold',
            'silver': 'MercadoLider',
        }

        try:
            db.execute("""
                INSERT INTO mp_competitors (org_id, platform, seller_id, nickname, rating,
                    completed_sales, badge, power_status, last_synced)
                VALUES (?, 'mercado_livre', ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(org_id, platform, seller_id)
                DO UPDATE SET nickname=?, rating=?, completed_sales=?,
                    badge=?, power_status=?, last_synced=datetime('now')
            """, (
                org_id, seller_id, nickname,
                round(positive * 5, 1), trans.get('completed', 0),
                badge_map.get(power, power or 'Seller padrao'), power,
                nickname, round(positive * 5, 1), trans.get('completed', 0),
                badge_map.get(power, power or 'Seller padrao'), power,
            ))
            db.commit()
        except Exception as e:
            db.close()
            return jsonify({'status': 'error', 'error': f'Erro ao salvar: {str(e)}'})

        db.close()
        return jsonify({'status': 'ok', 'seller': {'id': seller_id, 'nickname': nickname}})

    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)})


@app.route('/api/refresh-token')
def api_refresh_token():
    """Force refresh ML token and create missing tables."""
    org_id = 1
    mp = request.args.get('mp', 'mercado_livre')
    result = {'org_id': org_id, 'platform': mp}
    
    try:
        # Create mp_competitors table if missing
        from database import get_db
        db = get_db()
        db.execute("""CREATE TABLE IF NOT EXISTS mp_competitors (
            id INTEGER PRIMARY KEY AUTOINCREMENT, org_id INTEGER NOT NULL,
            platform TEXT NOT NULL DEFAULT 'mercado_livre', seller_id TEXT NOT NULL,
            nickname TEXT DEFAULT '', rating REAL DEFAULT 0, completed_sales INTEGER DEFAULT 0,
            price REAL DEFAULT 0, stock INTEGER DEFAULT 0, badge TEXT DEFAULT '',
            fulfillment INTEGER DEFAULT 0, sponsored INTEGER DEFAULT 0, sold_qty INTEGER DEFAULT 0,
            power_status TEXT DEFAULT '', last_synced TEXT DEFAULT (datetime('now')),
            UNIQUE(org_id, platform, seller_id))""")
        db.commit()
        result['table_created'] = True
        
        # Check existing tables
        tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        result['tables'] = tables
        db.close()
        
        # Force token refresh
        from sync_base import force_refresh_token, get_valid_token
        
        # First check current token
        token = get_valid_token(org_id, mp)
        if token:
            # Test if it works
            import urllib.request as ur
            try:
                req_ml = ur.Request(f'https://api.mercadolibre.com/users/me', headers={'Authorization': f'Bearer {token}'})
                me = json.loads(ur.urlopen(req_ml, timeout=10).read())
                result['token_valid'] = True
                result['user'] = me.get('nickname', me.get('id', ''))
                return jsonify({**result, 'status': 'ok'})
            except Exception as e:
                result['token_test'] = str(e)
        
        # Token invalid, try refresh
        new_token = force_refresh_token(org_id, mp)
        if new_token:
            result['refreshed'] = True
            result['new_token_preview'] = new_token[:15] + '...'
            # Test new token
            import urllib.request as ur
            try:
                req_ml = ur.Request(f'https://api.mercadolibre.com/users/me', headers={'Authorization': f'Bearer {new_token}'})
                me = json.loads(ur.urlopen(req_ml, timeout=10).read())
                result['token_valid'] = True
                result['user'] = me.get('nickname', me.get('id', ''))
            except Exception as e:
                result['new_token_test'] = str(e)
        else:
            result['refresh_failed'] = True
            result['message'] = 'Token refresh falhou. Reconecte a conta ML em Integracoes.'
        
        return jsonify({**result, 'status': 'ok' if result.get('token_valid') else 'needs_reconnect'})
    
    except Exception as e:
        import traceback
        return jsonify({**result, 'status': 'error', 'error': str(e), 'trace': traceback.format_exc()})



@app.route('/api/test-competitor-sync')
def test_competitor_sync():
    """Test _sync_competitors directly and return result."""
    import traceback, io, sys
    org_id = 1
    result = {}

    # Capture print output
    old_stdout = sys.stdout
    sys.stdout = buffer = io.StringIO()

    try:
        from sync_base import get_valid_token, force_refresh_token
        from sync_mercadolivre import _sync_competitors

        token = get_valid_token(org_id, 'mercado_livre')
        if not token:
            token_new = force_refresh_token(org_id, 'mercado_livre')
            if token_new:
                token = token_new
            else:
                result['error'] = 'No valid token'
                sys.stdout = old_stdout
                return jsonify(result)

        # Get user_id
        import urllib.request as ur
        headers_ml = {'Authorization': f'Bearer {token}'}
        req_ml = ur.Request('https://api.mercadolibre.com/users/me', headers=headers_ml)
        me = json.loads(ur.urlopen(req_ml, timeout=10).read())
        user_id = str(me.get('id', ''))
        result['user_id'] = user_id
        result['nickname'] = me.get('nickname', '')

        # Run competitor sync
        count = _sync_competitors(org_id, token, user_id)
        result['competitors_synced'] = count
        result['status'] = 'ok'

        # Check DB
        from database import get_db
        db = get_db()
        comps = db.execute(
            "SELECT seller_id, nickname, price, rating, sold_qty, badge FROM mp_competitors WHERE org_id=? AND platform='mercado_livre'",
            (org_id,)
        ).fetchall()
        db.close()
        result['db_competitors'] = [dict(c) for c in comps]

    except Exception as e:
        result['error'] = str(e)
        result['trace'] = traceback.format_exc()

    sys.stdout = old_stdout
    result['logs'] = buffer.getvalue()

    return jsonify(result)


@app.route('/api/marketplace-offers')
@login_required
def marketplace_offers():
    """Return real listing-type data + deal_ids for products (strategy tab)."""
    mp     = request.args.get('mp', 'mercado_livre')
    org_id = session.get('org_id', 1)
    if mp != 'mercado_livre':
        return jsonify({'error': 'platform_not_supported'})
    try:
        import urllib.request as ur
        from sync_base import get_valid_token
        from database import get_db

        token = get_valid_token(org_id, 'mercado_livre')
        if not token:
            return jsonify({'error': 'no_token'})

        ML = 'https://api.mercadolibre.com'
        h  = {'Authorization': f'Bearer {token}'}
        result = {}

        # ── Seller info ───────────────────────────────────────────────────
        me  = json.loads(ur.urlopen(ur.Request(f'{ML}/users/me', headers=h), timeout=10).read())
        uid = str(me.get('id', ''))
        result['seller'] = {
            'id': uid, 'nickname': me.get('nickname', ''),
            'level': (me.get('seller_reputation', {}) or {}).get('power_seller_status', '') or 'standard',
        }

        # ── Product listing types + available upgrades ────────────────────
        db = get_db()
        products = db.execute(
            "SELECT external_id, title, price, COALESCE(listing_type,'gold_special') as listing_type, category FROM mp_products WHERE org_id=? AND platform='mercado_livre' AND status='active' LIMIT 10",
            (org_id,)
        ).fetchall()
        db.close()

        # ML listing type labels (API returns technical names, we show Portuguese)
        LT_LABEL = {'gold_premium':'Diamante (Ouro Premium)','gold_pro':'Premium (Ouro Pro)',
                    'gold_special':'Clássico (Ouro Especial)','gold':'Ouro',
                    'silver':'Prata','bronze':'Bronze','free':'Grátis'}
        LT_EXPOSURE = {'gold_premium':'highest','gold_pro':'highest','gold_special':'high',
                       'gold':'high','silver':'mid','bronze':'low','free':'lowest'}
        LT_ORDER = ['free','bronze','silver','gold','gold_special','gold_pro','gold_premium']
        LT_FEE = {'gold_premium':16,'gold_pro':16,'gold_special':16,'gold':16,'silver':12,'bronze':10,'free':5}

        product_data = []
        deal_ids_all = []
        seen_cats = {}

        for row in products:
            p = dict(row)
            cat = p.get('category','')
            lt  = p.get('listing_type','gold_special')

            # Fetch available listing types per unique category
            avail = []
            if cat and cat not in seen_cats:
                try:
                    lt_req = ur.Request(f'{ML}/users/{uid}/available_listing_types?category_id={cat}', headers=h)
                    lt_data = json.loads(ur.urlopen(lt_req, timeout=8).read())
                    avail = [x.get('id') for x in lt_data.get('available', []) if x.get('id')]
                    seen_cats[cat] = avail
                except Exception:
                    seen_cats[cat] = []
            avail = seen_cats.get(cat, [])
            avail_sorted = sorted(avail, key=lambda x: LT_ORDER.index(x) if x in LT_ORDER else 99)

            # Check deal_ids from item
            item_deal_ids = []
            try:
                item_req = ur.Request(f'{ML}/items/{p["external_id"]}?attributes=deal_ids', headers=h)
                item_data = json.loads(ur.urlopen(item_req, timeout=6).read())
                item_deal_ids = item_data.get('deal_ids', [])
                deal_ids_all.extend(item_deal_ids)
            except Exception:
                pass

            product_data.append({
                'id':           p['external_id'],
                'title':        (p.get('title') or '')[:50],
                'price':        p.get('price', 0),
                'listing_type': lt,
                'lt_label':     LT_LABEL.get(lt, lt),
                'lt_fee':       LT_FEE.get(lt, 16),
                'available_lt': avail_sorted,
                'available_lt_labels': [LT_LABEL.get(x, x) for x in avail_sorted],
                'best_upgrade': next((x for x in reversed(avail_sorted) if (LT_ORDER.index(x) if x in LT_ORDER else 0) > (LT_ORDER.index(lt) if lt in LT_ORDER else 0)), None),
                'deal_ids':     item_deal_ids,
            })

        result['products'] = product_data
        result['active_deals'] = list(set(deal_ids_all))
        return jsonify(result)

    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()})



@app.route('/api/debug/ml-promos2')
def debug_ml_promos2():
    """Probe item-level promo data and additional ML endpoints."""
    import urllib.request as ur
    from sync_base import get_valid_token
    from database import get_db
    org_id = 1
    result = {}

    token = get_valid_token(org_id, 'mercado_livre')
    if not token:
        return jsonify({'error': 'no_token'})

    ML = 'https://api.mercadolibre.com'
    h = {'Authorization': f'Bearer {token}'}

    me_req = ur.Request(f'{ML}/users/me', headers=h)
    me = json.loads(ur.urlopen(me_req, timeout=10).read())
    user_id = str(me.get('id', ''))

    # Get our item IDs
    db = get_db()
    items = db.execute(
        "SELECT external_id, title, price, listing_type FROM mp_products WHERE org_id=? AND platform='mercado_livre' AND status='active' LIMIT 5",
        (org_id,)
    ).fetchall()
    db.close()
    item_ids = [dict(r)['external_id'] for r in items if dict(r).get('external_id')]
    result['item_ids'] = item_ids

    result['api_tests'] = []

    endpoints = []

    # Per-item promo endpoints
    if item_ids:
        iid = item_ids[0]
        endpoints += [
            (f'{ML}/items/{iid}/promotions', 'item promotions'),
            (f'{ML}/items/{iid}?attributes=id,promotions,deal_ids,listing_type_id,buying_mode', 'item deal_ids'),
        ]

    # Listing type details
    endpoints += [
        (f'{ML}/sites/MLB/listing_types/gold_pro', 'listing type gold_pro'),
        (f'{ML}/sites/MLB/listing_types/gold_premium', 'listing type gold_premium'),
        (f'{ML}/sites/MLB/listing_fees?price=200&listing_type_id=gold_pro&category_id=MLB1196', 'listing fees MLB1196 gold_pro'),
        (f'{ML}/users/{user_id}/available_listing_types?category_id=MLB1196', 'available listing types'),
        (f'{ML}/sites/MLB/promotions_types', 'promo types'),
        (f'{ML}/seller-promotions/users/{user_id}', 'seller-promotions root'),
        (f'https://api.mercadolibre.com/v2/promotions?user_id={user_id}&limit=5', 'v2 promotions'),
    ]

    for ep, label in endpoints:
        test = {'label': label, 'url': ep.replace(user_id, '{uid}').replace(item_ids[0] if item_ids else 'NOID', '{iid}')}
        try:
            req_ep = ur.Request(ep, headers=h)
            raw = ur.urlopen(req_ep, timeout=10).read()
            parsed = json.loads(raw)
            test['status'] = 'ok'
            if isinstance(parsed, list):
                test['type'] = 'list'; test['count'] = len(parsed)
                test['sample'] = parsed[:2]
            elif isinstance(parsed, dict):
                test['type'] = 'dict'; test['keys'] = list(parsed.keys())[:12]
                test['sample'] = {k: v for k, v in list(parsed.items())[:6]}
        except Exception as e:
            test['status'] = 'error'; test['error'] = str(e)[:150]
        result['api_tests'].append(test)

    return jsonify(result)


@app.route('/api/debug/ml-promos')
def debug_ml_promos():
    """Probe all available ML promotion APIs for this seller."""
    import urllib.request as ur
    from sync_base import get_valid_token
    org_id = 1
    result = {}

    token = get_valid_token(org_id, 'mercado_livre')
    if not token:
        return jsonify({'error': 'no_token'})

    ML = 'https://api.mercadolibre.com'
    h = {'Authorization': f'Bearer {token}'}

    # Get user_id
    me_req = ur.Request(f'{ML}/users/me', headers=h)
    me = json.loads(ur.urlopen(me_req, timeout=10).read())
    user_id = str(me.get('id', ''))
    result['user_id'] = user_id
    result['nickname'] = me.get('nickname', '')
    result['api_tests'] = []

    # Test multiple promo endpoints
    endpoints = [
        f'{ML}/seller-promotions/users/{user_id}/promotions?status=candidate&limit=20',
        f'{ML}/seller-promotions/users/{user_id}/promotions?status=published&limit=20',
        f'{ML}/users/{user_id}/classifieds_promotion_packs',
        f'{ML}/promotions?seller_id={user_id}&status=candidate&limit=10',
        f'{ML}/users/{user_id}/promotions?status=candidate',
        f'{ML}/deals/users/{user_id}',
    ]
    for ep in endpoints:
        test = {'url': ep.replace(user_id, '{uid}')}
        try:
            req_ep = ur.Request(ep, headers=h)
            raw = ur.urlopen(req_ep, timeout=10).read()
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                test['type'] = 'list'
                test['count'] = len(parsed)
                test['sample'] = parsed[:1]
            elif isinstance(parsed, dict):
                test['type'] = 'dict'
                test['keys'] = list(parsed.keys())[:10]
                test['count'] = len(parsed.get('results', parsed.get('promotions', parsed.get('promotion_packs', []))))
                results_key = next((k for k in ['results','promotions','promotion_packs','deals'] if k in parsed), None)
                if results_key:
                    test['sample'] = parsed[results_key][:1]
                else:
                    test['sample'] = {k: v for k, v in list(parsed.items())[:5]}
            test['status'] = 'ok'
        except Exception as e:
            test['status'] = 'error'
            test['error'] = str(e)[:120]
        result['api_tests'].append(test)

    return jsonify(result)


@app.route('/api/force-sync')
def force_sync():
    """Force a full re-sync (bypasses staleness check)."""
    org_id = 1
    mp = request.args.get('mp', 'mercado_livre')
    try:
        if mp == 'mercado_livre':
            from sync_mercadolivre import sync_all
            from sync_base import log_sync
            records = sync_all(org_id)
            log_sync(org_id, mp, 'full', 'success', records_synced=records or 0)
            return jsonify({'status': 'ok', 'records_synced': records, 'platform': mp})
        return jsonify({'status': 'error', 'error': f'Unknown platform: {mp}'})
    except Exception as e:
        import traceback
        return jsonify({'status': 'error', 'error': str(e), 'trace': traceback.format_exc()})


@app.route('/api/debug/competitors')
def debug_competitors():
    import traceback
    org_id = 1
    mp = request.args.get('mp', 'mercado_livre')
    result = {'org_id': org_id, 'marketplace': mp, 'steps': []}

    try:
        from sync_base import get_valid_token
        from database import get_db
        import urllib.request as ur

        token = get_valid_token(org_id, mp)
        if not token:
            result['steps'].append('No valid token')
            return jsonify(result)
        result['has_token'] = True
        result['token_preview'] = token[:15] + '...'
        result['steps'].append('Got token')

        # Get products from DB
        db = get_db()
        products = db.execute(
            "SELECT external_id, title, category, price, sold_qty, status FROM mp_products WHERE org_id=? AND platform='mercado_livre'",
            (org_id,)
        ).fetchall()
        db.close()
        result['products'] = [dict(p) for p in products]
        result['steps'].append(f'Found {len(products)} products')

        # Step 1: Multi-get our items to find catalog_product_id
        ML_API = 'https://api.mercadolibre.com'
        item_ids = [dict(p)['external_id'] for p in products if dict(p).get('external_id')]
        headers_ml = {'Authorization': f'Bearer {token}'}
        catalog_products = []

        if item_ids:
            ids_str = ','.join(item_ids[:20])
            try:
                req_ml = ur.Request(
                    f'{ML_API}/items?ids={ids_str}&attributes=id,catalog_product_id,category_id,seller_id',
                    headers=headers_ml
                )
                resp_ml = json.loads(ur.urlopen(req_ml, timeout=15).read())
                for entry in resp_ml:
                    if entry.get('code') == 200:
                        item = entry.get('body', {})
                        cat_prod = item.get('catalog_product_id')
                        result['steps'].append(f"Item {item.get('id')}: catalog={cat_prod}")
                        if cat_prod:
                            catalog_products.append(cat_prod)
            except Exception as e:
                result['steps'].append(f'Multi-get failed: {e}')

        result['catalog_products'] = catalog_products
        result['steps'].append(f'Found {len(catalog_products)} catalog products')

        # Step 2: Try catalog product items + also try /products/{id} for buy_box_winner
        competitor_items = []
        competitor_sellers = []  # Direct seller data from catalog
        seen_catalogs = set()

        for cp in catalog_products[:5]:
            if cp in seen_catalogs:
                continue
            seen_catalogs.add(cp)

            # Method A: Try /products/{cp} for buy_box_winner and pickers
            try:
                req_ml = ur.Request(f'{ML_API}/products/{cp}', headers=headers_ml)
                prod_data = json.loads(ur.urlopen(req_ml, timeout=15).read())

                # buy_box_winner has direct seller/item info
                bbw = prod_data.get('buy_box_winner', {})
                if bbw:
                    bbw_item_id = bbw.get('item_id', '')
                    bbw_seller_id = bbw.get('seller_id', '')
                    if bbw_item_id and str(bbw_item_id) not in item_ids:
                        competitor_items.append(str(bbw_item_id))
                        result['steps'].append(f'Buy box winner: item={bbw_item_id} seller={bbw_seller_id}')

                # pickers has all competing items
                pickers = prod_data.get('pickers', [])
                if pickers:
                    result['steps'].append(f'Catalog {cp}: {len(pickers)} pickers')

                # main_features, attributes etc
                result['catalog_raw_keys'] = list(prod_data.keys())

            except Exception as e:
                result['steps'].append(f'Product {cp} detail: {e}')

            # Method B: Try /products/{cp}/items
            try:
                req_ml = ur.Request(f'{ML_API}/products/{cp}/items?status=active&limit=20', headers=headers_ml)
                raw = ur.urlopen(req_ml, timeout=15).read()
                resp_ml = json.loads(raw)

                # Log raw response structure
                if isinstance(resp_ml, list):
                    result['steps'].append(f'Catalog {cp} items: list of {len(resp_ml)}')
                    if resp_ml:
                        first = resp_ml[0]
                        if isinstance(first, dict):
                            result['steps'].append(f'First item keys: {list(first.keys())[:10]}')
                            # If items have seller info directly, extract it
                            seller = first.get('seller', first.get('seller_id', ''))
                            result['steps'].append(f'First item seller: {seller}')
                elif isinstance(resp_ml, dict):
                    result['steps'].append(f'Catalog {cp} items: dict keys={list(resp_ml.keys())[:10]}')
                    result['catalog_items_raw'] = str(resp_ml)[:500]

                items = resp_ml if isinstance(resp_ml, list) else resp_ml.get('results', resp_ml.get('items', []))
                for ci in items:
                    ci_id = ci if isinstance(ci, str) else ci.get('id', ci.get('item_id', ''))
                    if ci_id and str(ci_id) not in item_ids:
                        competitor_items.append(str(ci_id))
                    # Try to extract seller directly from catalog response
                    if isinstance(ci, dict):
                        s_id = ci.get('seller_id') or (ci.get('seller', {}) or {}).get('id')
                        if s_id:
                            competitor_sellers.append({
                                'seller_id': str(s_id),
                                'item_id': ci.get('id', ci.get('item_id', '')),
                                'price': ci.get('price', 0),
                            })
            except Exception as e:
                result['steps'].append(f'Catalog {cp} items FAILED: {e}')

        result['competitor_sellers_from_catalog'] = competitor_sellers

        result['competitor_item_ids'] = competitor_items[:20]

        # Step 3: If catalog didn't work, try category search fallback
        if not competitor_items:
            categories = list(set(dict(p).get('category', '') for p in products if dict(p).get('category')))
            for cat_id in categories[:2]:
                if not cat_id:
                    continue
                try:
                    search_url = f'{ML_API}/sites/MLB/search?category={cat_id}&limit=10&sort=sold_quantity_desc&access_token={token}'
                    req_ml = ur.Request(search_url, headers={'User-Agent': 'Mozilla/5.0'})
                    resp_ml = json.loads(ur.urlopen(req_ml, timeout=15).read())
                    items = resp_ml.get('results', [])
                    result['steps'].append(f'Category search {cat_id}: {len(items)} items')
                    for it in items:
                        if isinstance(it, dict):
                            it_id = it.get('id', '')
                            if it_id and str(it_id) not in item_ids:
                                competitor_items.append(str(it_id))
                except Exception as e:
                    result['steps'].append(f'Category search {cat_id} FAILED: {e}')

        # Deduplicate competitor_items
        competitor_items = list(dict.fromkeys(competitor_items))
        result['competitor_item_ids'] = competitor_items[:20]
        result['steps'].append(f'{len(competitor_items)} unique competitor items after dedup')

        # Get our user ID first
        our_user_id = None
        try:
            me_req = ur.Request(f'{ML_API}/users/me', headers=headers_ml)
            me_data = json.loads(ur.urlopen(me_req, timeout=10).read())
            our_user_id = str(me_data.get('id', ''))
            result['our_user_id'] = our_user_id
        except Exception as e:
            result['steps'].append(f'Could not get our user: {e}')

        # Step 4: Multi-get competitor items for seller details
        if competitor_items:
            ids_str = ','.join(competitor_items[:20])
            result['steps'].append(f'Fetching items: {ids_str}')
            try:
                req_ml = ur.Request(f'{ML_API}/items?ids={ids_str}', headers=headers_ml)
                raw_resp = ur.urlopen(req_ml, timeout=15).read()
                resp_ml = json.loads(raw_resp)
                result['steps'].append(f'Got {len(resp_ml)} item responses')
                competitors = []
                for entry in resp_ml:
                    code = entry.get('code')
                    if code == 200:
                        item = entry.get('body', {})
                        seller = item.get('seller', {}) or {}
                        seller_id = str(seller.get('id', ''))
                        nickname = seller.get('nickname', '?')
                        is_ours = seller_id == our_user_id if our_user_id else False
                        result['steps'].append(f'Item {item.get("id")}: seller={seller_id} nick={nickname} ours={is_ours}')
                        if not is_ours and seller_id:
                            competitors.append({
                                'item_id': item.get('id'),
                                'seller_id': seller.get('id'),
                                'nickname': nickname,
                                'price': item.get('price'),
                                'title': (item.get('title', '')[:60])
                            })
                    else:
                        msg = entry.get('body', {})
                        if isinstance(msg, dict):
                            msg = msg.get('message', str(msg))
                        result['steps'].append(f'Item response code={code}: {str(msg)[:80]}')
                result['found_competitors'] = competitors
                result['steps'].append(f'Found {len(competitors)} competitor sellers (excluding ours)')
            except Exception as e:
                result['steps'].append(f'Competitor detail fetch FAILED: {e}')
                import traceback as tb
                result['fetch_trace'] = tb.format_exc()
        else:
            result['steps'].append('No competitor items found via any method')

        # Check existing DB competitors
        db = get_db()
        db_comps = db.execute(
            "SELECT seller_id, nickname, price, rating, sold_qty FROM mp_competitors WHERE org_id=? AND platform=?",
            (org_id, mp)
        ).fetchall()
        db.close()
        result['db_competitors'] = [dict(c) for c in db_comps]
        result['competitors_count'] = len(db_comps)

    except Exception as e:
        result['error'] = str(e)
        result['trace'] = traceback.format_exc()

    return jsonify(result)

@app.route('/integrations')
@login_required
def integrations():
    try:
        return _integrations_inner()
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

def _integrations_inner():
    from integrations import INTEGRATIONS_CATALOG
    from oauth_manager import get_all_integrations, is_app_configured
    org_id    = session.get('org_id', 1)
    connected_map = get_all_integrations(org_id)  # returns dict keyed by platform
    platforms = []
    for key, info in INTEGRATIONS_CATALOG.items():
        conn = connected_map.get(key, {})
        platforms.append({**info, 'key': key,
                          'connected':    conn.get('status') == 'connected',
                          'configured':   is_app_configured(key),
                          'account_name': conn.get('account_name', ''),
                          'last_sync':    conn.get('last_sync', '')})
    return render_template('integrations_hub.html', platforms=platforms)

@app.route('/api/debug/integrations')
@login_required
def debug_integrations():
    from database import get_db as _gdb
    org_id = session.get('org_id', 1)
    db = _gdb()
    rows = db.execute('SELECT id, org_id, platform, status, account_id, account_name, last_sync FROM api_integrations WHERE org_id=?', (org_id,)).fetchall()
    db.close()
    return jsonify({'org_id': org_id, 'integrations': [dict(r) for r in rows]})

@app.route('/integrations/connect/<platform>')
@login_required
def connect_integration(platform):
    from oauth_manager import is_app_configured, OAUTH_APPS
    from integrations import INTEGRATIONS_CATALOG
    cat = INTEGRATIONS_CATALOG.get(platform, {})
    if cat.get('auth_type') == 'oauth2':
        if not is_app_configured(platform):
            env_vars = []
            if platform == 'mercado_livre':
                env_vars = ['ML_APP_ID', 'ML_APP_SECRET']
            elif platform == 'meta_ads':
                env_vars = ['META_APP_ID', 'META_APP_SECRET']
            elif platform == 'google_ads':
                env_vars = ['GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET']
            elif platform == 'tiktok_ads':
                env_vars = ['TIKTOK_APP_ID', 'TIKTOK_APP_SECRET']
            elif platform == 'tiktok_shop':
                env_vars = ['TIKTOK_SHOP_APP_KEY', 'TIKTOK_SHOP_APP_SECRET']
            elif platform == 'google_analytics':
                env_vars = ['GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET']
            elif platform == 'bling':
                env_vars = ['BLING_CLIENT_ID', 'BLING_CLIENT_SECRET']
            return render_template('oauth_not_configured.html',
                                   platform_name=cat.get('name', platform),
                                   env_vars=env_vars)
        # Mostra tela intermediaria para escolher/nomear a conta
        return render_template('oauth_pre_connect.html',
                               platform_key=platform,
                               platform_name=cat.get('name', platform),
                               icon=cat.get('icon', '🔗'),
                               color=cat.get('color', '#6c63ff'),
                               text_color=cat.get('text_color', '#fff'),
                               steps=cat.get('steps', []))
    # Amazon, Shopee and TikTok Shop use the friendly guided wizard
    if platform in ('amazon', 'shopee', 'tiktok_shop'):
        return redirect(f'/integrations/connect/{platform}/wizard?step=1')

    # Other API key platforms: Re-fetch all platforms so the grid doesn't disappear
    from oauth_manager import get_all_integrations, is_app_configured as isc
    org_id = session.get('org_id', 1)
    connected_map = get_all_integrations(org_id)
    all_platforms = []
    for k, info in INTEGRATIONS_CATALOG.items():
        conn = connected_map.get(k, {})
        all_platforms.append({**info, 'key': k,
                              'connected': conn.get('status') == 'connected',
                              'configured': isc(k),
                              'account_name': conn.get('account_name', ''),
                              'last_sync': conn.get('last_sync', '')})
    return render_template('integrations_hub.html', platforms=all_platforms, api_key_platform=platform, catalog_item=cat)


@app.route('/integrations/connect/<platform>/start', methods=['POST'])
@login_required
def connect_integration_start(platform):
    from oauth_manager import build_auth_url, is_app_configured, revoke_ml_grant
    from integrations import INTEGRATIONS_CATALOG
    cat = INTEGRATIONS_CATALOG.get(platform, {})
    if cat.get('auth_type') != 'oauth2' or not is_app_configured(platform):
        return redirect(url_for('integrations'))
    account_label = request.form.get('account_label', '').strip()
    if account_label:
        session['pending_account_label'] = account_label
        session['pending_account_platform'] = platform
    org_id = session.get('org_id', 1)
    # Revoga grant existente para forcar o ML a pedir login novamente
    if platform == 'mercado_livre':
        revoke_ml_grant(org_id)
    url = build_auth_url(platform, org_id, request.host)
    return redirect(url)


@app.route('/integrations/callback/<platform>')
def oauth_callback(platform):
    from oauth_manager import exchange_code_for_token, save_integration
    from oauth_manager import fetch_ml_account_info, fetch_meta_account_info, fetch_google_account_info
    code  = request.args.get('code')
    state = request.args.get('state', '')
    error = request.args.get('error')
    if error:
        return render_template('settings.html', success=False, msg=f'Erro: {error}')
    if not code:
        return render_template('settings.html', success=False, msg='Código de autorização não recebido')
    try:
        org_id = int(state) if state.isdigit() else session.get('org_id', 1)
    except Exception:
        org_id = session.get('org_id', 1)
    try:
        token_data, token_error = exchange_code_for_token(platform, code, request.host)
        if token_error or not token_data:
            return render_template('settings.html', success=False, msg=f'Erro ao obter token: {token_error}')
        access_token = token_data.get('access_token', '')
        account_info = {}
        if platform == 'mercado_livre' and access_token:
            account_info = fetch_ml_account_info(access_token)
        elif platform == 'meta_ads' and access_token:
            account_info = fetch_meta_account_info(access_token)
        elif platform == 'google_ads' and access_token:
            account_info = fetch_google_account_info(access_token)
        # Usa o label definido pelo usuario na tela pre-OAuth, se disponivel
        pending_label = session.pop('pending_account_label', '')
        pending_platform = session.pop('pending_account_platform', '')
        if pending_label and pending_platform == platform:
            account_info['custom_label'] = pending_label
        save_integration(org_id, platform, token_data, account_info)
        account_name = pending_label or account_info.get('nickname') or account_info.get('name') or platform
        return render_template('settings.html', success=True,
                               msg=f'Conectado com sucesso a {platform}!',
                               account_name=account_name)
    except Exception as e:
        return render_template('settings.html', success=False, msg=f'Erro ao conectar: {str(e)}')

@app.route('/integrations/connect/<platform>/wizard')
@login_required
def connect_wizard(platform):
    """Guided wizard for Amazon and Shopee — user-friendly step-by-step."""
    step = int(request.args.get('step', 1))
    color = '#ff9900' if platform == 'amazon' else '#ee4d2d'
    icon  = '📦' if platform == 'amazon' else '🧡'

    if platform == 'amazon':
        config = {
            'platform_name': 'Amazon Seller',
            'subtitle': 'Conecte sua loja Amazon Seller Central ao Sellvance em 2 passos',
            'icon': icon, 'color': color,
            'steps': ['O que voce precisa', 'Conectar'],
            'what_you_need': [
                {'icon': '📧', 'label': 'E-mail da conta Amazon Seller',
                 'desc': 'O e-mail que voce usa para entrar no Seller Central.',
                 'link': 'https://sellercentral.amazon.com.br'},
                {'icon': '🔑', 'label': 'Senha da conta Amazon Seller',
                 'desc': 'A mesma senha que voce usa para acessar o Seller Central. Armazenada de forma segura e criptografada.',
                 'link': None},
            ],
            'fields': [
                {'key': 'account_email', 'label': 'E-mail da conta Amazon Seller',
                 'type': 'email', 'placeholder': 'Ex: vendedor@minhaloja.com.br',
                 'required': True, 'help': 'O mesmo e-mail que voce usa para entrar no Seller Central.', 'help_link': None},
                {'key': 'account_password', 'label': 'Senha da conta Amazon Seller',
                 'type': 'password', 'placeholder': '                ',
                 'required': True,
                 'help': 'A mesma senha do Seller Central. Armazenada de forma segura.', 'help_link': None},
            ],
            'hidden_fields': [
                {'key': 'marketplace_id', 'value': 'A2Q3Y263D00KWC'},
            ],
        }

    elif platform == 'shopee':
        config = {
            'platform_name': 'Shopee',
            'subtitle': 'Conecte sua loja Shopee ao Sellvance em 2 passos simples',
            'icon': icon, 'color': color,
            'steps': ['O que voce precisa', 'Conectar'],
            'what_you_need': [
                {'icon': '📧', 'label': 'E-mail da conta Shopee',
                 'desc': 'O e-mail que voce usa para entrar no Painel do Vendedor Shopee.',
                 'link': 'https://seller.shopee.com.br'},
                {'icon': '🔑', 'label': 'Senha da conta Shopee',
                 'desc': 'A mesma senha que voce usa para acessar o Painel do Vendedor.',
                 'link': None},
            ],
            'fields': [
                {'key': 'account_email', 'label': 'E-mail da conta Shopee',
                 'type': 'email', 'placeholder': 'Ex: vendedor@minhaloja.com.br',
                 'required': True, 'help': 'O mesmo e-mail que voce usa para entrar no Painel Shopee.', 'help_link': None},
                {'key': 'account_password', 'label': 'Senha da conta Shopee',
                 'type': 'password', 'placeholder': '                ',
                 'required': True,
                 'help': 'A mesma senha do Painel do Vendedor. Armazenada de forma segura.', 'help_link': None},
            ],
            'hidden_fields': [],
        }

    elif platform == 'tiktok_shop':
        config = {
            'platform_name': 'TikTok Shop',
            'subtitle': 'Conecte sua loja TikTok Shop ao Sellvance em 2 passos simples',
            'icon': '🎵', 'color': '#ff0050',
            'steps': ['O que voce precisa', 'Conectar'],
            'what_you_need': [
                {'icon': '📧', 'label': 'E-mail da conta TikTok Shop',
                 'desc': 'O e-mail que voce usa para entrar no TikTok Shop Seller Center.',
                 'link': 'https://seller-br.tiktok.com'},
                {'icon': '🔑', 'label': 'Senha da conta TikTok Shop',
                 'desc': 'A mesma senha que voce usa para acessar o Seller Center.',
                 'link': None},
            ],
            'fields': [
                {'key': 'account_email', 'label': 'E-mail da conta TikTok Shop',
                 'type': 'email', 'placeholder': 'Ex: vendedor@minhaloja.com.br',
                 'required': True, 'help': 'O mesmo e-mail que voce usa no Seller Center.', 'help_link': None},
                {'key': 'account_password', 'label': 'Senha da conta TikTok Shop',
                 'type': 'password', 'placeholder': '                ',
                 'required': True,
                 'help': 'A mesma senha do Seller Center. Armazenada de forma segura.', 'help_link': None},
            ],
            'hidden_fields': [],
        }

    else:
        return redirect('/integrations')

    return render_template('connect_wizard.html',
        current_step=step,
        platform_key=platform,
        **config)


@app.route('/integrations/save-keys/<platform>', methods=['POST'])
@login_required
def save_api_keys(platform):
    from oauth_manager import save_api_key_integration
    org_id = session.get('org_id', 1)
    fields = {k: v for k, v in request.form.items() if v.strip()}
    try:
        save_api_key_integration(org_id, platform, fields)
        return render_template('settings.html', success=True,
                               msg=f'{platform} conectado via API Keys!',
                               account_name=fields.get('seller_id', platform))
    except Exception as e:
        return render_template('settings.html', success=False, msg=f'Erro: {str(e)}')

@app.route('/integrations/disconnect/<platform>', methods=['POST'])
@login_required
def disconnect_integration(platform):
    from oauth_manager import disconnect_integration as do_disconnect
    org_id = session.get('org_id', 1)
    do_disconnect(org_id, platform)
    return redirect(url_for('integrations'))

@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    msg = None
    if request.method == 'POST':
        db       = get_db()
        name     = request.form.get('name', '')
        password = request.form.get('password', '')
        if name:
            db.execute('UPDATE users SET name = ? WHERE id = ?', (name, session['user_id']))
            session['user_name'] = name
        if password and len(password) >= 6:
            db.execute('UPDATE users SET password_hash = ? WHERE id = ?',
                       (hash_password(password), session['user_id']))
        db.commit()
        msg = 'Alterações salvas com sucesso!'
    return render_template('integrations.html', msg=msg)

# ── API endpoints for dashboard charts ──────────────────────────────────────

@app.route('/api/revenue-chart')
@login_required
def api_revenue_chart():
    db     = get_db()
    org_id = session.get('org_id', 1)
    rows   = db.execute('''
        SELECT date(ordered_at) as day, SUM(revenue) as rev
        FROM orders WHERE org_id = ?
        GROUP BY day ORDER BY day DESC LIMIT 30
    ''', (org_id,)).fetchall()
    rows = list(reversed(rows))
    return jsonify({'labels': [r['day'] for r in rows], 'values': [r['rev'] for r in rows]})

@app.route('/api/channel-chart')
@login_required
def api_channel_chart():
    db     = get_db()
    org_id = session.get('org_id', 1)
    rows   = db.execute('''
        SELECT channel, SUM(revenue) as rev
        FROM orders WHERE org_id = ?
        GROUP BY channel ORDER BY rev DESC
    ''', (org_id,)).fetchall()
    return jsonify({'labels': [r['channel'] for r in rows], 'values': [r['rev'] for r in rows]})

@app.route('/ranking/pause/<int:campaign_id>', methods=['POST'])
@login_required
def pause_campaign(campaign_id):
    db     = get_db()
    org_id = session.get('org_id', 1)
    db.execute('UPDATE ad_campaigns SET status = ?, paused_by_ai = 1, ai_note = ? WHERE id = ? AND org_id = ?',
               ('paused', 'Pausada pela IA — score abaixo do mínimo', campaign_id, org_id))
    db.commit()
    return jsonify({'status': 'ok'})

@app.route('/ranking/resume/<int:campaign_id>', methods=['POST'])
@login_required
def resume_campaign(campaign_id):
    db     = get_db()
    org_id = session.get('org_id', 1)
    db.execute('UPDATE ad_campaigns SET status = ?, paused_by_ai = 0, ai_note = ? WHERE id = ? AND org_id = ?',
               ('active', '', campaign_id, org_id))
    db.commit()
    return jsonify({'status': 'ok'})



# -- Trafego Pago dashboard --

@app.route('/traffic')
@login_required
def traffic():
    db = get_db()
    org_id = session.get('org_id', 1)
    
    # Date filters
    date_start = request.args.get('date_start', '')
    date_end = request.args.get('date_end', '')

    # Only Meta and Google campaigns (TikTok stays in Marketplaces)
    sql = "SELECT * FROM ad_campaigns WHERE org_id = ? AND platform IN ('meta', 'google')"
    params = [org_id]
    if date_start:
        sql += " AND date >= ?"
        params.append(date_start)
    if date_end:
        sql += " AND date <= ?"
        params.append(date_end)
    campaigns_raw = db.execute(sql, params).fetchall()
    
    # Calculate metrics for each campaign
    campaigns = []
    total_spend = 0
    total_revenue = 0
    total_clicks = 0
    total_impressions = 0
    total_conversions = 0
    
    for c in campaigns_raw:
        c_dict = dict(c)
        m = calc_metrics(c_dict)
        s = score_campaign(c_dict, m)
        if s['score'] >= 75:
            action = 'scale'
        elif s['score'] >= 50:
            action = 'optimize'
        else:
            action = 'pause'
        campaigns.append({**c_dict, **m, **s, 'action': action})
        total_spend += c_dict.get('spend', 0)
        total_revenue += c_dict.get('revenue', 0)
        total_clicks += c_dict.get('clicks', 0)
        total_impressions += c_dict.get('impressions', 0)
        total_conversions += c_dict.get('conversions', 0)
    
    campaigns.sort(key=lambda x: x['score'], reverse=True)
    
    # Overall KPIs
    roas = round(total_revenue / max(total_spend, 1), 2)
    ctr = round(total_clicks / max(total_impressions, 1) * 100, 2)
    cpc = round(total_spend / max(total_clicks, 1), 2)
    cpa = round(total_spend / max(total_conversions, 1), 2)
    conv_rate = round(total_conversions / max(total_clicks, 1) * 100, 2)
    profit = total_revenue - total_spend
    
    # AI analysis (returns tuple: results, insights, global_roas)
    try:
        ai_results, ai_insights, ai_global_roas = analyze_all(campaigns_raw)
    except Exception:
        ai_results, ai_insights, ai_global_roas = [], [], 0
    
    # By platform
    platforms = {}
    for c in campaigns:
        p = c.get('platform', 'Outro')
        if p not in platforms:
            platforms[p] = {'spend': 0, 'revenue': 0, 'campaigns': 0}
        platforms[p]['spend'] += c.get('spend', 0)
        platforms[p]['revenue'] += c.get('revenue', 0)
        platforms[p]['campaigns'] += 1
    for p in platforms:
        platforms[p]['roas'] = round(platforms[p]['revenue'] / max(platforms[p]['spend'], 1), 2)
    
    kpis = {
        'total_spend': total_spend,
        'total_revenue': total_revenue,
        'roas': roas,
        'ctr': ctr,
        'cpc': cpc,
        'cpa': cpa,
        'conv_rate': conv_rate,
        'profit': profit,
        'total_campaigns': len(campaigns),
        'active_campaigns': sum(1 for c in campaigns if c.get('status') == 'active'),
    }
    
    return render_template('trafego_pago.html', 
                         campaigns=campaigns, kpis=kpis, 
                         platforms=platforms, insights=ai_insights, ai_global_roas=ai_global_roas)

# -- AI Suggestions endpoint for Marketplaces --

@app.route('/marketplaces/ai-suggestions', methods=['POST'])
@login_required
def marketplace_ai_suggestions():
    """Gera sugestoes inteligentes baseadas nos dados do marketplace."""
    try:
        from marketplace_intel import (COMPETITORS, MY_PRODUCTS, MP_ADS_DATA,
                                        RETURNS_DATA, ACCOUNT_HEALTH,
                                        analyze_competitive_position,
                                        analyze_mp_ads, get_keyword_opportunities)

        data = request.get_json() or {}
        mp = data.get('marketplace', 'mercado_livre')

        actions = []

        # 1. Analise competitiva
        comp = analyze_competitive_position(mp)
        for opp in comp.get('opportunities', []):
            actions.append({
                'category': 'Concorrencia',
                'title': opp.get('title', ''),
                'description': opp.get('text', ''),
                'impact': opp.get('impact', 'Medio'),
                'time_to_implement': opp.get('urgency', 'Esta semana'),
                'expected_result': 'Aproveite esta oportunidade para ganhar market share'
            })
        for rec in comp.get('recommendations', []):
            actions.append({
                'category': 'Preco',
                'title': rec.get('title', ''),
                'description': rec.get('text', ''),
                'impact': 'Alto' if rec.get('priority') == 'alta' else 'Medio',
                'time_to_implement': 'Esta semana',
                'expected_result': 'Melhoria na competitividade de preco'
            })

        # 2. Analise de anuncios
        ads = analyze_mp_ads(mp)
        for ad in ads:
            if ad.get('action') == 'scale':
                actions.append({
                    'category': 'Anuncios',
                    'title': 'Escalar: ' + ad['name'],
                    'description': 'ROAS ' + str(ad['roas']) + 'x com ACoS ' + str(ad['acos']) + '%. Aumente o budget em 20-30% para maximizar retorno.',
                    'impact': 'Alto',
                    'time_to_implement': 'Imediato',
                    'expected_result': 'Aumento de receita proporcional ao budget adicional'
                })
            elif ad.get('action') == 'pause':
                actions.append({
                    'category': 'Anuncios',
                    'title': 'Pausar: ' + ad['name'],
                    'description': 'ACoS de ' + str(ad['acos']) + '% esta muito alto. ROAS de apenas ' + str(ad['roas']) + 'x. Pause e redirecione o budget.',
                    'impact': 'Alto',
                    'time_to_implement': 'Imediato',
                    'expected_result': 'Economia de R$' + str(int(ad['spend'])) + ' em gastos ineficientes'
                })
            elif ad.get('action') == 'optimize':
                actions.append({
                    'category': 'Anuncios',
                    'title': 'Otimizar: ' + ad['name'],
                    'description': 'CTR de ' + str(ad['ctr']) + '% e CPC de R$' + str(ad['cpc']) + '. Teste novos criativos e palavras-chave para melhorar conversao.',
                    'impact': 'Medio',
                    'time_to_implement': 'Esta semana',
                    'expected_result': 'Reducao de 15-20% no ACoS'
                })

        # 3. Keywords
        keywords = get_keyword_opportunities(mp)
        high_opp = [k for k in keywords if k.get('opportunity') in ('muito alto', 'alto')]
        if high_opp:
            kw_names = ', '.join(k['kw'] for k in high_opp[:3])
            actions.append({
                'category': 'Palavras-chave',
                'title': str(len(high_opp)) + ' palavras-chave com alta oportunidade',
                'description': 'Foque em: ' + kw_names + '. Volume alto e concorrencia baixa/media.',
                'impact': 'Alto',
                'time_to_implement': 'Esta semana',
                'expected_result': 'Aumento de 20-40% no trafego organico e pago'
            })

        # 4. Saude da conta
        health = ACCOUNT_HEALTH.get(mp, {})
        for alert in health.get('alerts', []):
            actions.append({
                'category': 'Saude da Conta',
                'title': 'Alerta de saude da conta',
                'description': alert,
                'impact': 'Alto',
                'time_to_implement': 'Imediato',
                'expected_result': 'Prevencao de suspensao ou perda de badge'
            })

        # 5. Devolucoes
        returns = RETURNS_DATA.get(mp, {})
        if returns.get('return_rate', 0) > 5:
            top_reason = returns.get('reasons', [{}])[0]
            actions.append({
                'category': 'Devolucoes',
                'title': 'Taxa de devolucao alta: ' + str(returns['return_rate']) + '%',
                'description': 'Principal motivo: ' + top_reason.get('reason', 'N/A') + ' (' + str(top_reason.get('pct', 0)) + '%). Revise descricao e fotos do produto.',
                'impact': 'Alto',
                'time_to_implement': 'Esta semana',
                'expected_result': 'Economia de R$' + str(int(returns.get('refunded_revenue', 0))) + ' em reembolsos'
            })

        # 6. Estoque baixo
        my = MY_PRODUCTS.get(mp, {})
        if my.get('stock_20l', 0) <= 10:
            actions.append({
                'category': 'Estoque',
                'title': 'Estoque critico: Cooler 20L (' + str(my.get('stock_20l', 0)) + ' un)',
                'description': 'Estoque abaixo de 10 unidades. Risco de ruptura e perda de ranking.',
                'impact': 'Alto',
                'time_to_implement': 'Imediato',
                'expected_result': 'Evitar ruptura de estoque e perda de posicionamento'
            })

        return jsonify({'actions': actions})

    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500



# ── API Sync endpoint ─────────────────────────────────────────────────────────

@app.route('/api/sync/<platform>', methods=['POST'])
@login_required
def api_sync(platform):
    org_id = session.get('org_id', 1)
    try:
        if platform == 'mercado_livre':
            from sync_mercadolivre import sync_all
            records = sync_all(org_id)
        elif platform == 'meta_ads':
            from sync_meta_ads import sync_all
            records = sync_all(org_id)
        else:
            return jsonify({'status': 'error', 'msg': f'Sync not implemented for {platform}'}), 400

        from sync_base import get_last_sync_info
        info = get_last_sync_info(org_id, platform)
        return jsonify({'status': 'ok', 'records': records, 'last_sync': info})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'msg': str(e)}), 500


@app.route('/api/sync-status/<platform>')
@login_required
def api_sync_status(platform):
    org_id = session.get('org_id', 1)
    from sync_base import get_last_sync_info, is_stale
    info = get_last_sync_info(org_id, platform)
    stale = is_stale(org_id, platform)
    return jsonify({'last_sync': info, 'is_stale': stale})



@app.route('/api/debug/live-data')
@login_required
def debug_live_data():
    """Show exactly what live data functions return for the template."""
    from marketplace_intel import (get_my_products_live, get_account_health_live,
                                   get_returns_live, is_platform_synced,
                                   MY_PRODUCTS, ACCOUNT_HEALTH, RETURNS_DATA)
    from oauth_manager import get_integration
    org_id = session.get('org_id', 1)
    mp = request.args.get('mp', 'mercado_livre')
    result = {'org_id': org_id, 'marketplace': mp}

    # Check connection
    integration = get_integration(org_id, mp)
    is_connected = integration and integration.get('status') == 'connected'
    result['is_connected'] = is_connected

    # What do the live functions return?
    try:
        result['products_live'] = get_my_products_live(org_id, mp)
    except Exception as e:
        result['products_error'] = str(e)
        import traceback
        result['products_trace'] = traceback.format_exc()

    try:
        result['health_live'] = get_account_health_live(org_id, mp)
    except Exception as e:
        result['health_error'] = str(e)
        import traceback
        result['health_trace'] = traceback.format_exc()

    try:
        result['returns_live'] = get_returns_live(org_id, mp)
    except Exception as e:
        result['returns_error'] = str(e)
        import traceback
        result['returns_trace'] = traceback.format_exc()

    result['is_synced'] = is_platform_synced(org_id, mp)

    # Compare with demo data
    result['products_demo'] = MY_PRODUCTS.get(mp, {})
    result['health_demo'] = ACCOUNT_HEALTH.get(mp, {})

    # Raw DB data
    try:
        db = get_db()
        products = db.execute("SELECT * FROM mp_products WHERE org_id=? AND platform=?", (org_id, mp)).fetchall()
        result['raw_products'] = [dict(p) for p in products]
        health = db.execute("SELECT * FROM mp_account_health WHERE org_id=? AND platform=?", (org_id, mp)).fetchone()
        result['raw_health'] = dict(health) if health else None
        returns = db.execute("SELECT * FROM mp_returns WHERE org_id=? AND platform=?", (org_id, mp)).fetchone()
        result['raw_returns'] = dict(returns) if returns else None
        db.close()
    except Exception as e:
        result['raw_db_error'] = str(e)
        import traceback
        result['raw_db_trace'] = traceback.format_exc()

    return jsonify(result)


@app.route('/api/debug/db-info')
@login_required
def debug_db_info():
    """Show database path and volume info."""
    import os
    from database import DB_PATH, DATA_DIR
    result = {
        'DB_PATH': DB_PATH,
        'DATA_DIR': DATA_DIR,
        'RAILWAY_VOLUME_MOUNT_PATH': os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', 'NOT SET'),
        'db_exists': os.path.exists(DB_PATH),
        'db_size': os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0,
        'wal_exists': os.path.exists(DB_PATH + '-wal'),
        'wal_size': os.path.getsize(DB_PATH + '-wal') if os.path.exists(DB_PATH + '-wal') else 0,
        'shm_exists': os.path.exists(DB_PATH + '-shm'),
        'data_dir_contents': os.listdir(DATA_DIR) if os.path.isdir(DATA_DIR) else 'NOT A DIR',
        'cwd': os.getcwd(),
        'app_dir': os.path.dirname(os.path.abspath(__file__)),
    }

    # Check integrations
    db = get_db()
    rows = db.execute('SELECT id, org_id, platform, status, account_name FROM api_integrations ORDER BY id').fetchall()
    result['integrations'] = [dict(r) for r in rows]

    # Check if there are any tables
    tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    result['tables'] = [r[0] for r in tables]
    db.close()

    return jsonify(result)

# ══ ROTAS DE RELATORIOS EXPORTAVEIS ═══════════════════════════════

@app.route('/api/debug/sync-diag')
@login_required
def sync_diagnostic():
    """Diagnostic endpoint to debug sync issues."""
    from oauth_manager import get_integration
    org_id = session.get('org_id', 1)
    result = {'org_id': org_id, 'platforms': {}}

    for platform in ['mercado_livre', 'meta_ads', 'google_ads', 'tiktok_shop', 'amazon', 'tiktok_ads']:
        info = {'connected': False}
        try:
            integration = get_integration(org_id, platform)
            if integration:
                info['connected'] = integration.get('status') == 'connected'
                info['status'] = integration.get('status', 'unknown')
                info['account_name'] = integration.get('account_name', '')
                info['account_id'] = integration.get('account_id', '')
                config = integration.get('config', {})
                info['has_access_token'] = bool(config.get('access_token'))
                info['has_refresh_token'] = bool(config.get('refresh_token'))
                info['token_len'] = len(config.get('access_token', ''))
                info['user_id'] = config.get('user_id', '')
            else:
                info['status'] = 'no_row'
        except Exception as e:
            info['error'] = str(e)
        result['platforms'][platform] = info

    # Check ALL api_integrations rows to debug
    try:
        db = get_db()
        all_rows = db.execute('SELECT id, org_id, platform, status, account_id, account_name FROM api_integrations ORDER BY id').fetchall()
        result['all_integrations'] = [dict(r) for r in all_rows]
        db.close()
    except Exception as e:
        result['all_integrations_error'] = str(e)

    # Check sync_log
    try:
        db = get_db()
        rows = db.execute('SELECT * FROM sync_log ORDER BY finished_at DESC LIMIT 10').fetchall()
        result['sync_log'] = [dict(r) for r in rows]
        db.close()
    except Exception as e:
        result['sync_log_error'] = str(e)

    # Check table counts
    try:
        db = get_db()
        for table in ['mp_products', 'mp_account_health', 'mp_returns', 'mp_ads', 'orders', 'contacts', 'organizations', 'users']:
            try:
                count = db.execute(f'SELECT COUNT(*) as c FROM {table}').fetchone()['c']
                result[f'count_{table}'] = count
            except Exception as e:
                result[f'count_{table}'] = f'ERROR: {e}'
        db.close()
    except Exception as e:
        result['table_count_error'] = str(e)

    # Try manual sync
    if request.args.get('try_sync') == '1':
        try:
            from sync_mercadolivre import sync_all as ml_sync
            from sync_base import run_sync_if_needed
            sync_result = run_sync_if_needed(org_id, 'mercado_livre', ml_sync, max_age=0)
            result['manual_sync_result'] = sync_result
        except Exception as e:
            import traceback
            result['manual_sync_error'] = str(e)
            result['manual_sync_trace'] = traceback.format_exc()

    return jsonify(result)




@app.route('/reports/dashboard')
@login_required
def report_dashboard():
    from reports import generate_dashboard_report
    fmt = request.args.get('format', 'xlsx')
    org_id = session.get('org_id', 1)
    date_start = request.args.get('date_start', '')
    date_end = request.args.get('date_end', '')
    buf, filename, mimetype = generate_dashboard_report(org_id, fmt, date_start=date_start, date_end=date_end)
    return send_file(buf, as_attachment=True, download_name=filename, mimetype=mimetype)

@app.route('/reports/traffic')
@login_required
def report_traffic():
    from reports import generate_traffic_report
    fmt = request.args.get('format', 'xlsx')
    org_id = session.get('org_id', 1)
    date_start = request.args.get('date_start', '')
    date_end = request.args.get('date_end', '')
    buf, filename, mimetype = generate_traffic_report(org_id, fmt, date_start=date_start, date_end=date_end)
    return send_file(buf, as_attachment=True, download_name=filename, mimetype=mimetype)

@app.route('/reports/crm')
@login_required
def report_crm():
    from reports import generate_crm_report
    fmt = request.args.get('format', 'xlsx')
    org_id = session.get('org_id', 1)
    date_start = request.args.get('date_start', '')
    date_end = request.args.get('date_end', '')
    buf, filename, mimetype = generate_crm_report(org_id, fmt, date_start=date_start, date_end=date_end)
    return send_file(buf, as_attachment=True, download_name=filename, mimetype=mimetype)

@app.route('/reports/marketplaces')
@login_required
def report_marketplaces():
    from reports import generate_marketplaces_report
    fmt = request.args.get('format', 'xlsx')
    org_id = session.get('org_id', 1)
    mp = request.args.get('mp', 'mercado_livre')
    date_start = request.args.get('date_start', '')
    date_end = request.args.get('date_end', '')
    buf, filename, mimetype = generate_marketplaces_report(org_id, mp, fmt, date_start=date_start, date_end=date_end)
    return send_file(buf, as_attachment=True, download_name=filename, mimetype=mimetype)

if __name__ == '__main__':
    app.run(debug=True)
