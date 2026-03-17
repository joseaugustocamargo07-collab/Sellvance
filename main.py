# Sellvance App — main server
from flask import Flask, render_template, request, session, redirect, url_for, jsonify, send_file
from database import get_db
from auth import login_required, verify_password, hash_password
from traffic_ai import analyze_all, calc_metrics, score_campaign
import os
import json

app = Flask(__name__, template_folder='.')
app.secret_key = os.environ.get('SECRET_KEY', 'sellvance-secret-2026-change-in-prod')

_db_ready = False

@app.before_request
def ensure_db_ready():
    global _db_ready
    if not _db_ready:
        _db_ready = True
        try:
            import os as _os
            from database import init_db, migrate_db, DB_PATH, DATA_DIR
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

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email    = request.form.get('email', '')
        password = request.form.get('password', '')
        db       = get_db()
        user     = db.execute('SELECT * FROM users WHERE email = ?', (email,)).fetchone()
        if user and verify_password(password, user['password_hash']):
            session['user_id']   = user['id']
            session['user_name'] = user['name']
            session['org_id']    = user['org_id']
            session['org_name']  = user['org_name']
            return redirect(url_for('dashboard'))
        return render_template('login.html', error='Email ou senha inválidos')
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
    contacts        = db.execute('SELECT * FROM contacts WHERE org_id = ? ORDER BY ltv DESC', (org_id,)).fetchall()
    total_contacts  = len(contacts)
    total_ltv       = sum(c['ltv'] for c in contacts)
    repeat_buyers   = sum(1 for c in contacts if c['total_orders'] > 1)
    recompra_rate   = round(repeat_buyers / max(total_contacts, 1) * 100, 1)
    rfm = {}
    for c in contacts:
        seg      = c['rfm_segment']
        rfm[seg] = rfm.get(seg, 0) + 1
    search      = request.args.get('search', '')
    page        = request.args.get('page', 1, type=int)
    per_page    = 50
    total_pages = max(1, (total_contacts + per_page - 1) // per_page)
    page        = max(1, min(page, total_pages))
    start       = (page - 1) * per_page
    paged       = contacts[start:start + per_page]
    return render_template('crm.html', contacts=paged, total_contacts=total_contacts,
                           total_ltv=total_ltv, recompra_rate=recompra_rate, rfm=rfm,
                           search=search, cur_page=page, total_pages=total_pages)

@app.route('/ranking')
@login_required
def ranking():
    db             = get_db()
    org_id         = session.get('org_id', 1)
    campaigns_raw  = db.execute('SELECT * FROM ad_campaigns WHERE org_id = ?', (org_id,)).fetchall()
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
    return render_template('ranking.html', campaigns=campaigns, revenue_wasted=revenue_wasted)

@app.route('/marketplaces')
@login_required
def marketplaces():
    try:
        return _marketplaces_inner()
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

def _marketplaces_inner():
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
    all_mp = [
        {'id': 'mercado_livre', 'name': 'Mercado Livre', 'icon': '\U0001f6d2', 'color': '#ffe600'},
        {'id': 'amazon',        'name': 'Amazon',        'icon': '\U0001f4e6', 'color': '#ff9900'},
        {'id': 'tiktok_shop',   'name': 'TikTok Shop',   'icon': '\U0001f3b5', 'color': '#ff0050'},
    ]
    # Check if platform is connected and trigger sync if needed
    org_id = session.get('org_id', 1)
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

        # Build comp_analysis from real product data to match template fields
        real_products = get_real_products_list(org_id, mp)
        if real_products:
            prices = [p['price'] for p in real_products if p.get('price') and p['price'] > 0]
            max_p = max(prices) if prices else 0
            min_p = min(prices) if prices else 0
            avg_p = round(sum(prices) / len(prices), 2) if prices else 0
            my_p = my_product.get('price_32l', avg_p)
            if avg_p > 0:
                if my_p > avg_p * 1.1:
                    pos = 'acima'
                elif my_p < avg_p * 0.9:
                    pos = 'abaixo'
                else:
                    pos = 'na_media'
            else:
                pos = 'na_media'
            analysis = {
                'max_price_32l': max_p * 1.15,
                'min_price_32l': min_p * 0.85,
                'avg_price_32l': avg_p,
                'price_position': pos,
                'opportunities': [],
            }
            my_product['_products'] = real_products
            my_product['_live'] = True
        else:
            analysis = analyze_competitive_position(mp)
            my_product['_live'] = True

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
    return render_template('traffic.html', mp=mp, tab=tab, all_mp=all_mp, health=health, is_live=is_live, sync_info=sync_info, account_name=account_name,
                           competitors=competitors, my=my_product, comp_analysis=analysis,
                           ads=ads, returns=returns, keywords=keywords, stock_items=stock_items,
                           mp_totals=mp_totals, date_start=date_start, date_end=date_end)






@app.route('/api/force-sync')
@login_required
def force_sync():
    """Force a full re-sync for the connected ML account."""
    org_id = session.get('org_id', 1)
    mp = request.args.get('mp', 'mercado_livre')
    try:
        from sync_base import run_sync_if_needed
        records = run_sync_if_needed(org_id, mp, force=True)
        return jsonify({'status': 'ok', 'records_synced': records, 'platform': mp})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)})


@app.route('/api/debug/competitors')
def debug_competitors():
    """Debug endpoint for competitor search."""
    import traceback
    org_id = 1
    mp = request.args.get('mp', 'mercado_livre')
    result = {'org_id': org_id, 'marketplace': mp, 'steps': []}

    try:
        from marketplace_intel import search_ml_competitors, get_keywords_from_products
        from sync_base import get_valid_token

        # Step 1: Get token
        token = get_valid_token(org_id, mp)
        result['has_token'] = bool(token)
        result['token_preview'] = token[:15] + '...' if token else None
        result['steps'].append('Got token' if token else 'No token')

        # Step 2: Check products
        from database import get_db
        db = get_db()
        products = db.execute(
            "SELECT title, category, price, sold_qty, status FROM mp_products WHERE org_id=? AND platform=?",
            (org_id, mp)
        ).fetchall()
        result['products'] = [dict(p) for p in products]
        result['steps'].append(f'Found {len(products)} products')

        # Step 3: Try category search
        if products:
            top = dict(products[0])
            category = top.get('category', '')
            result['top_category'] = category

            if token and category:
                try:
                    url = f"https://api.mercadolibre.com/sites/MLB/search?category={category}&limit=5&sort=sold_quantity_desc"
                    import urllib.request as ur
                    req = ur.Request(url, headers={
                        'Authorization': f'Bearer {token}',
                        'User-Agent': 'Sellvance/1.0',
                        'Accept': 'application/json',
                    })
                    resp_data = json.loads(ur.urlopen(req, timeout=15).read())
                    result['category_search_total'] = resp_data.get('paging', {}).get('total', 0)
                    result['category_search_results'] = len(resp_data.get('results', []))
                    items = []
                    for item in resp_data.get('results', [])[:3]:
                        seller = item.get('seller', {})
                        items.append({
                            'title': item.get('title', '')[:60],
                            'price': item.get('price'),
                            'seller': seller.get('nickname'),
                            'seller_id': seller.get('id'),
                        })
                    result['sample_items'] = items
                    result['steps'].append(f'Category search OK: {len(resp_data.get("results",[]))} results')
                except Exception as e:
                    result['category_search_error'] = str(e)
                    result['steps'].append(f'Category search FAILED: {e}')

        # Step 4: Try full competitor search
        try:
            competitors = search_ml_competitors(org_id, mp, token)
            result['competitors_count'] = len(competitors)
            result['competitors'] = competitors
            result['steps'].append(f'Competitor search returned {len(competitors)}')
        except Exception as e:
            result['competitor_error'] = str(e)
            result['competitor_traceback'] = traceback.format_exc()
            result['steps'].append(f'Competitor search FAILED: {e}')

        # Step 5: Try keywords
        try:
            keywords = get_keywords_from_products(org_id, mp)
            result['keywords_count'] = len(keywords)
            result['keywords'] = keywords
            result['steps'].append(f'Keywords returned {len(keywords)}')
        except Exception as e:
            result['keywords_error'] = str(e)
            result['steps'].append(f'Keywords FAILED: {e}')

        db.close()

    except Exception as e:
        result['error'] = str(e)
        result['traceback'] = traceback.format_exc()

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
    # Re-fetch all platforms so the grid doesn't disappear
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
