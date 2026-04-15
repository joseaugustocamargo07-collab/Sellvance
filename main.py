# Sellvance App — main server
from flask import Flask, render_template, request, session, redirect, url_for, jsonify, send_file
from database import get_db
from auth import login_required, verify_password, hash_password
from traffic_ai import analyze_all, calc_metrics, score_campaign
import os
import json
from functools import wraps

# ── Plan access control ──────────────────────────────────────────────────────
# Plan definitions: which pages each plan can access
PLAN_ACCESS = {
    'marketplaces': {
        'label': 'Marketplaces',
        'pages': ['dashboard', 'marketplaces', 'crm', 'automacao', 'logistica', 'vulnerability', 'mini-loja', 'pagamentos', 'integrations', 'settings'],
        'integrations': ['amazon', 'shopee', 'mercado_livre', 'tiktok_shop'],
    },
    'marketing': {
        'label': 'Marketing',
        'pages': ['dashboard', 'traffic', 'ranking', 'integrations', 'settings'],
        'integrations': ['meta', 'google', 'tiktok', 'google_analytics'],
    },
    'completo': {
        'label': 'Completo',
        'pages': ['dashboard', 'traffic', 'ranking', 'marketplaces', 'crm', 'automacao', 'logistica', 'vulnerability', 'mini-loja', 'pagamentos', 'integrations', 'settings'],
        'integrations': ['amazon', 'shopee', 'mercado_livre', 'tiktok_shop', 'meta', 'google', 'tiktok', 'google_analytics'],
    },
    'growth': {  # legacy — treat as completo
        'label': 'Completo',
        'pages': ['dashboard', 'traffic', 'ranking', 'marketplaces', 'crm', 'automacao', 'logistica', 'vulnerability', 'mini-loja', 'pagamentos', 'integrations', 'settings'],
        'integrations': ['amazon', 'shopee', 'mercado_livre', 'tiktok_shop', 'meta', 'google', 'tiktok', 'google_analytics'],
    },
}

def get_org_plan(org_id=None):
    """Return the plan name for the current org."""
    if org_id is None:
        org_id = session.get('org_id', 1)
    db = get_db()
    row = db.execute('SELECT plan FROM organizations WHERE id=?', (org_id,)).fetchone()
    db.close()
    return row['plan'] if row else 'completo'

def plan_has_access(plan, page):
    """Check if a plan can access a given page."""
    cfg = PLAN_ACCESS.get(plan, PLAN_ACCESS['completo'])
    return page in cfg['pages']

def plan_required(page_name):
    """Decorator that checks if the org's plan allows access to this page."""
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if 'user_id' not in session:
                return redirect(url_for('login'))
            plan = get_org_plan()
            if not plan_has_access(plan, page_name):
                plan_label = PLAN_ACCESS.get(plan, {}).get('label', plan)
                return render_template('plan_blocked.html', page=page_name, plan=plan, plan_label=plan_label), 403
            return f(*args, **kwargs)
        return decorated
    return decorator

app = Flask(__name__, template_folder='.')

@app.context_processor
def inject_plan():
    """Make plan info available in all templates."""
    plan = session.get('plan', 'completo')
    plan_cfg = PLAN_ACCESS.get(plan, PLAN_ACCESS['completo'])
    return {
        'user_plan': plan,
        'plan_label': plan_cfg['label'],
        'plan_pages': plan_cfg['pages'],
        'plan_integrations': plan_cfg.get('integrations', []),
    }


# ── Filtro Jinja2 para formato monetario brasileiro ──────────────────────────
def _brl_filter(value, decimals=2):
    """Formata numero no padrao brasileiro: 1.234,56"""
    try:
        value = float(value or 0)
    except (TypeError, ValueError):
        return '0'
    if decimals == 0:
        formatted = f"{value:,.0f}"
    else:
        formatted = f"{value:,.{decimals}f}"
    # Swap US format to BR: 1,234.56 -> 1.234,56
    # Step 1: comma -> temp, Step 2: dot -> comma, Step 3: temp -> dot
    formatted = formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
    return formatted


app.jinja_env.filters['brl'] = _brl_filter
app.jinja_env.filters['brl0'] = lambda v: _brl_filter(v, 0)
app.jinja_env.filters['brl2'] = lambda v: _brl_filter(v, 2)


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
            # Bootstrap modulos de auto-melhoria (telemetria, flags, pricing, insights)
            try:
                import telemetry, feature_flags, pricing_ai, auto_insights, whatsapp_agent, buybox_monitor
                import fraud_detector, content_ai, cohort_analytics
                telemetry.ensure_tables()
                feature_flags.ensure_tables()
                pricing_ai.ensure_tables()
                auto_insights.ensure_tables()
                whatsapp_agent.ensure_tables()
                buybox_monitor.ensure_tables()
                fraud_detector.ensure_tables()
                content_ai.ensure_tables()
                cohort_analytics.ensure_tables()
                telemetry.register_request_hooks(app)
                print("[startup] auto-improvement modules loaded")
            except Exception as _e:
                print(f"[startup] auto-improvement bootstrap warning: {_e}")
        except Exception as e:
            import traceback
            traceback.print_exc()

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'app': 'Sellvance CRM'}), 200


@app.route('/healthz')
def healthz():
    """Health check simples para Railway/load balancers."""
    import time as _t
    return jsonify({'status': 'ok', 'ts': int(_t.time())}), 200


@app.route('/healthz/deep')
def healthz_deep():
    """Health check profundo com error rate e DB status."""
    import time as _t
    from database import get_db as _gdb
    result = {'status': 'ok', 'ts': int(_t.time()), 'checks': {}}
    try:
        _d = _gdb()
        _d.execute('SELECT 1').fetchone()
        _d.close()
        result['checks']['database'] = 'ok'
    except Exception as _e:
        result['checks']['database'] = f'error: {str(_e)[:100]}'
        result['status'] = 'degraded'
    try:
        _d = _gdb()
        _total = _d.execute(
            "SELECT COUNT(*) c FROM events_log WHERE event_type='request' AND created_at > datetime('now', '-5 minutes')"
        ).fetchone()
        _errors = _d.execute(
            "SELECT COUNT(*) c FROM events_log WHERE event_type='request' AND status_code >= 500 AND created_at > datetime('now', '-5 minutes')"
        ).fetchone()
        _d.close()
        _t_c = _total['c'] if _total else 0
        _e_c = _errors['c'] if _errors else 0
        _rate = (_e_c / _t_c) if _t_c > 0 else 0
        result['checks']['error_rate'] = {
            'total': _t_c, 'errors': _e_c, 'rate': round(_rate, 4), 'ok': _rate <= 0.05
        }
        if _rate > 0.05:
            result['status'] = 'degraded'
    except Exception as _e:
        result['checks']['error_rate'] = 'unavailable'
    try:
        _d = _gdb()
        _stuck = _d.execute(
            "SELECT COUNT(*) c FROM api_integrations WHERE status='connected' AND (last_sync IS NULL OR last_sync < datetime('now', '-6 hours'))"
        ).fetchone()
        _d.close()
        result['checks']['stale_syncs'] = _stuck['c'] if _stuck else 0
    except Exception:
        result['checks']['stale_syncs'] = 'unknown'
    return jsonify(result), 200 if result['status'] == 'ok' else 503

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
            session['plan']      = get_org_plan(user['org_id'])
            return redirect(url_for('dashboard'))
        return render_template('login.html', error='Email ou senha invalidos')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/crm')
@login_required
@plan_required('crm')
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
@plan_required('ranking')
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
@plan_required('marketplaces')
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
    # Also add any connected MARKETPLACE platform not in default list
    # Exclude ads/marketing platforms — they belong to the Marketing plan
    _ads_platforms = {'meta_ads', 'google_ads', 'tiktok_ads', 'google_analytics', 'meta', 'google', 'tiktok'}
    for _pid, _conn in _all_integ.items():
        if _conn.get('status') == 'connected' and _pid not in _default_order and _pid not in _ads_platforms:
            _info = _MP_CATALOG.get(_pid, {'name': _pid.title(), 'icon': '\U0001f6cd', 'color': '#6b7280'})
            all_mp.append({
                'id': _pid, 'name': _info['name'], 'icon': _info['icon'],
                'color': _info['color'], 'is_connected': True,
                'account_name': _conn.get('account_name', ''),
            })
    # Check if platform is connected and trigger sync if needed (background)
    integration = get_integration(org_id, mp)
    is_connected = integration and integration.get('status') == 'connected'
    sync_info = None

    # Fire-and-forget sync in background thread to avoid blocking page load
    import threading as _th
    def _bg_sync(_oid, _mp):
        try:
            if _mp == 'amazon':
                from sync_amazon import sync_all as _sf
            elif _mp == 'mercado_livre':
                from sync_mercadolivre import sync_all as _sf
            elif _mp == 'shopee':
                from shopee_api import sync_all as _sf
            else:
                return
            run_sync_if_needed(_oid, _mp, _sf, max_age=60)
        except Exception as _e:
            print(f"[bg_sync] {_mp} error: {_e}")

    if is_connected and mp in ('amazon', 'mercado_livre', 'shopee'):
        _th.Thread(target=_bg_sync, args=(org_id, mp), daemon=True).start()
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
        _awaiting_sync = False
    elif is_connected and not is_platform_synced(org_id, mp):
        # ── CONNECTED BUT NO DATA YET ───────────────────────
        # Platform is connected but has never synced successfully.
        # Show empty state instead of misleading demo data.
        health      = {'score': 0, 'metrics': {}, 'alerts': []}
        my_product  = {'name': '', 'price_32l': 0, 'reviews': 0, 'rating': 0}
        competitors = []
        analysis    = {'max_price_32l': 0, 'min_price_32l': 0, 'avg_price_32l': 0,
                       'price_position': 'na_media', 'opportunities': []}
        ads         = []
        returns     = {'total_orders': 0, 'total_returns': 0, 'return_rate': 0,
                       'refunded_revenue': 0, 'reasons': []}
        keywords    = []
        is_live     = False
        # Flag to show "awaiting sync" banner in template
        _awaiting_sync = True
    else:
        # ── DEMO DATA MODE (not connected) ───────────────────
        health      = ACCOUNT_HEALTH.get(mp, {'score': 0, 'metrics': {}, 'alerts': []})
        my_product  = MY_PRODUCTS.get(mp, {})
        competitors = COMPETITORS.get(mp, [])
        analysis    = analyze_competitive_position(mp)
        ads         = analyze_mp_ads(mp)
        returns     = RETURNS_DATA.get(mp, {})
        keywords    = get_keyword_opportunities(mp)
        is_live     = False
        _awaiting_sync = False

    db          = get_db()
    stock_items = db.execute('SELECT * FROM stock_items WHERE org_id = ? AND marketplace = ?',
                             (org_id, mp)).fetchall()

    # Aggregate marketplace totals from orders (with date filter)
    mp_totals = {}
    for m_id in ['mercado_livre', 'amazon', 'tiktok_shop', 'shopee']:
        # Check if this marketplace is connected - if so, only count real orders
        m_integration = get_integration(org_id, m_id)
        m_connected = m_integration and m_integration.get('status') == 'connected'

        if m_connected and is_platform_synced(org_id, m_id):
            # Only real orders (with external_id from sync)
            mp_totals[m_id] = get_real_orders_totals(org_id, m_id, date_start, date_end)
        elif m_connected:
            # Connected but no sync yet — show zeros, not demo data
            mp_totals[m_id] = {'revenue': 0, 'orders': 0, 'total_orders': 0}
        else:
            # Not connected — demo mode
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

        # If no competitors in DB, use prices from the competitor list (demo/seed data)
        if _avg_p == 0 and competitors:
            _comp_prices = [c.get('price_32l', 0) for c in competitors if c.get('price_32l', 0) > 0]
            if _comp_prices:
                _avg_p = round(sum(_comp_prices) / len(_comp_prices), 2)
                _min_p = round(min(_comp_prices), 2)
                _max_p = round(max(_comp_prices), 2)

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
                           strategy_scores=strategy_scores, rebid_recs=rebid_recs,
                           awaiting_sync=_awaiting_sync)






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
    """Force a full re-sync (bypasses staleness check).
    Add ?clean=1 to delete old data first and resync from scratch."""
    org_id = 1
    mp = request.args.get('mp', 'mercado_livre')
    clean = request.args.get('clean', '0') == '1'
    try:
        if clean:
            # Delete old orders/data so they get re-synced with correct revenue values
            db = get_db()
            # Always clean sync_log for this platform when clean=1
            db.execute("DELETE FROM sync_log WHERE org_id=? AND platform=?", (org_id, mp))
            if mp == 'mercado_livre':
                # Delete ML orders that have external_id (will be re-fetched)
                db.execute("DELETE FROM orders WHERE org_id=? AND marketplace='mercado_livre' AND external_id IS NOT NULL AND external_id != ''", (org_id,))
                # Also delete demo orders (no external_id) for ML marketplace
                db.execute("DELETE FROM orders WHERE org_id=? AND marketplace='mercado_livre' AND (external_id IS NULL OR external_id = '')", (org_id,))
                # Clear sync log to force fresh sync
                db.execute("DELETE FROM sync_log WHERE org_id=? AND platform=?", (org_id, mp))
            db.commit()

        if mp == 'mercado_livre':
            from sync_mercadolivre import sync_all
            from sync_base import log_sync, get_valid_token
            import traceback as _tb

            # Step 1: Get token
            try:
                token = get_valid_token(org_id, 'mercado_livre')
                token_ok = True
                token_len = len(token) if token else 0
            except Exception as e:
                token_ok = False
                token_len = 0
                return jsonify({'status': 'error', 'step': 'get_token', 'error': str(e), 'trace': _tb.format_exc()})

            # Step 2: Test API — force refresh if 401
            try:
                from sync_base import api_request as _api_req
                api_data = _api_req('https://api.mercadolibre.com/users/me',
                                    headers={'Authorization': f'Bearer {token}'})
                api_ok = True
                user_id = api_data.get('id', '')
                nickname = api_data.get('nickname', '')
            except Exception as e:
                # If 401, force a token refresh and retry
                if '401' in str(e):
                    try:
                        from oauth_manager import get_integration, refresh_access_token, save_integration
                        integ = get_integration(org_id, 'mercado_livre')
                        cfg = integ.get('config', {}) if integ else {}
                        rt = cfg.get('refresh_token', '')
                        if rt:
                            new_data, err = refresh_access_token('mercado_livre', rt)
                            if not err and new_data.get('access_token'):
                                cfg['access_token'] = new_data['access_token']
                                if new_data.get('refresh_token'):
                                    cfg['refresh_token'] = new_data['refresh_token']
                                if new_data.get('expires_in'):
                                    cfg['expires_in'] = new_data['expires_in']
                                save_integration(org_id, 'mercado_livre', cfg)
                                token = cfg['access_token']
                                token_len = len(token)
                                # Retry API test
                                api_data = _api_req('https://api.mercadolibre.com/users/me',
                                                    headers={'Authorization': f'Bearer {token}'})
                                api_ok = True
                                user_id = api_data.get('id', '')
                                nickname = api_data.get('nickname', '')
                            else:
                                api_ok = False
                                api_data = f"Refresh failed: {err}"
                                user_id = ''
                                nickname = ''
                        else:
                            api_ok = False
                            api_data = "No refresh_token available"
                            user_id = ''
                            nickname = ''
                    except Exception as e2:
                        api_ok = False
                        api_data = f"Refresh retry failed: {e2}"
                        user_id = ''
                        nickname = ''
                else:
                    api_ok = False
                    api_data = str(e)
                    user_id = ''
                    nickname = ''

            # Step 3: Run sync
            try:
                records = sync_all(org_id)
                log_sync(org_id, mp, 'full', 'success', records_synced=records or 0)
                sync_error = None
            except Exception as e:
                records = 0
                sync_error = str(e)
                sync_trace = _tb.format_exc()

            return jsonify({
                'status': 'ok' if not sync_error else 'error',
                'records_synced': records,
                'platform': mp,
                'cleaned': clean,
                'token_ok': token_ok,
                'token_len': token_len,
                'api_ok': api_ok,
                'user_id': user_id,
                'nickname': nickname,
                'api_response_keys': list(api_data.keys()) if isinstance(api_data, dict) else str(api_data)[:200],
                'sync_error': sync_error,
            })
        if mp == 'amazon':
            from sync_amazon import sync_all as amazon_sync_all
            from sync_base import log_sync
            import traceback as _tb_amz
            import os as _os
            # Debug: check credentials availability
            from oauth_manager import get_integration as _gi
            amz_integ = _gi(org_id, 'amazon')
            amz_cfg = amz_integ.get('config', {}) if amz_integ else {}
            debug_info = {
                'connected': bool(amz_integ and amz_integ.get('status') == 'connected'),
                'has_client_id': bool(amz_cfg.get('client_id')),
                'has_refresh_token': bool(amz_cfg.get('refresh_token')),
                'has_seller_id': bool(amz_cfg.get('seller_id')),
                'has_aws_key': bool(_os.environ.get('AMAZON_AWS_ACCESS_KEY')),
                'has_aws_secret': bool(_os.environ.get('AMAZON_AWS_SECRET_KEY')),
            }
            try:
                records = amazon_sync_all(org_id)
                if records and records > 0:
                    log_sync(org_id, mp, 'full', 'success', records_synced=records)
                return jsonify({'status': 'ok', 'records_synced': records, 'platform': mp, 'debug': debug_info})
            except Exception as e:
                return jsonify({'status': 'error', 'records_synced': 0, 'platform': mp,
                                'error': str(e), 'trace': _tb_amz.format_exc()[:500], 'debug': debug_info})
        if mp == 'shopee':
            from shopee_api import sync_all as shopee_sync_all
            result = shopee_sync_all(org_id)
            records = result.get('products', 0) + result.get('orders', 0)
            try:
                from sync_base import log_sync
                log_sync(org_id, mp, 'full', 'success', records_synced=records)
            except Exception:
                pass
            return jsonify({'status': 'ok', 'records_synced': records, 'platform': mp, 'detail': result})
        return jsonify({'status': 'error', 'error': f'Unknown platform: {mp}'})
    except Exception as e:
        import traceback
        return jsonify({'status': 'error', 'error': str(e), 'trace': traceback.format_exc()})


@app.route('/api/setup-mini-loja')
def setup_mini_loja():
    """Setup Mini Loja with simulated products for demo."""
    import json as _j
    from database import get_db
    try:
        org_id = 1
        db = get_db()
        # Create or update config
        existing = db.execute('SELECT id FROM mini_loja_config WHERE org_id=?', (org_id,)).fetchone()
        if existing:
            db.execute("UPDATE mini_loja_config SET is_active=1, whatsapp='5511999999999', store_name='Primeplas Coolers', banner_text='Entrega rapida em todo Brasil! Compre direto pelo WhatsApp' WHERE org_id=?", (org_id,))
        else:
            db.execute("INSERT INTO mini_loja_config (org_id,slug,store_name,whatsapp,is_active,banner_text) VALUES (?,?,?,?,?,?)",
                       (org_id, 'primeplas-coolers', 'Primeplas Coolers', '5511999999999', 1, 'Entrega rapida em todo Brasil! Compre direto pelo WhatsApp'))
        # Add all active products
        products = db.execute('SELECT id FROM mp_products WHERE org_id=? AND status=?', (org_id, 'active')).fetchall()
        count = 0
        for p in products:
            try:
                db.execute('INSERT OR REPLACE INTO mini_loja_products (org_id,mp_product_id,is_visible) VALUES (?,?,1)', (org_id, p['id']))
                count += 1
            except Exception:
                pass
        db.commit()
        db.close()
        return jsonify({'ok': True, 'products_added': count, 'url': '/loja/primeplas-coolers'})
    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()[:500]})


@app.route('/api/simulate/amazon')
def simulate_amazon_data():
    """Insert realistic Amazon test data to verify the full pipeline works."""
    import random, json as _j, traceback as _tb
    from datetime import datetime, timedelta
    from database import get_db
    try:
        org_id = 1
        db = get_db()
        # Limpar dados anteriores de simulacao
        db.execute("DELETE FROM orders WHERE org_id=? AND marketplace='amazon'", (org_id,))
        db.execute("DELETE FROM mp_products WHERE org_id=? AND platform='amazon'", (org_id,))
        products = [
            ('B0CX23VBGR', 'Fone Bluetooth TWS com Cancelamento de Ruido', 189.90, 45),
            ('B0DFKP9TQ7', 'Smartwatch Fitness Monitor Cardiaco IP68', 259.90, 32),
            ('B0CWL8R5N3', 'Carregador Portatil 20000mAh USB-C PD 65W', 139.90, 78),
            ('B0D3JM7VK2', 'Camera de Seguranca Wi-Fi 2K Visao Noturna', 219.90, 23),
            ('B0CRNP6D8L', 'Caixa de Som Bluetooth 30W Prova DAgua', 299.90, 51),
            ('B0D7HNWX4Q', 'Mouse Gamer RGB 12000 DPI Sem Fio', 169.90, 67),
            ('B0CK9PLR2M', 'Ring Light 12pol com Tripe e Suporte Celular', 119.90, 89),
            ('B0D1FVWN6T', 'Hub USB-C 7 em 1 HDMI 4K Ethernet', 199.90, 41),
            ('B0CNX8KJ5R', 'Teclado Mecanico 65pct Hot-Swap RGB', 279.90, 28),
            ('B0D5GTRM3K', 'Webcam Full HD 1080p Microfone Integrado', 149.90, 54),
            ('B0CYN2HP7W', 'Suporte Notebook Aluminio Ajustavel', 89.90, 112),
            ('B0D8JLQW1V', 'Headset Gamer 7.1 Surround USB', 229.90, 36),
        ]
        prod_count = 0
        for asin, title, price, stock in products:
            try:
                db.execute(
                    "INSERT OR REPLACE INTO mp_products (org_id,platform,external_id,title,price,stock_qty,status) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (org_id, 'amazon', asin, title, price, stock, 'active'))
                prod_count += 1
            except Exception:
                pass
        statuses_pool = ['delivered'] * 75 + ['pending'] * 10 + ['shipped'] * 10 + ['cancelled'] * 5
        now = datetime.now()
        order_rows = []
        for day_offset in range(240):
            date = now - timedelta(days=day_offset)
            n_orders = random.randint(2, 6)
            if date.weekday() in (4, 5):
                n_orders += random.randint(1, 3)
            for i in range(n_orders):
                prod = random.choice(products)
                asin, title, price, _ = prod
                qty = random.choices([1, 2, 3], weights=[70, 25, 5])[0]
                gmv = round(price * qty, 2)
                revenue = round(gmv * random.uniform(0.72, 0.88), 2)
                cost = round(gmv * random.uniform(0.35, 0.50), 2)
                status = random.choice(statuses_pool) if day_offset > 3 else random.choice(['pending', 'shipped'])
                order_date = date.replace(hour=random.randint(6, 23), minute=random.randint(0, 59), second=random.randint(0, 59))
                ext_id = f"408-{random.randint(1000000,9999999)}-{random.randint(1000000,9999999)}"
                order_rows.append((org_id, None, 'amazon', ext_id, status, gmv, revenue, cost, 'organic',
                                   order_date.strftime('%Y-%m-%d %H:%M:%S')))
        db.executemany(
            "INSERT OR REPLACE INTO orders (org_id,contact_id,marketplace,external_id,status,gmv,revenue,cost,channel,ordered_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            order_rows)
        order_count = len(order_rows)
        try:
            db.execute("DELETE FROM mp_account_health WHERE org_id=? AND platform=?", (org_id, 'amazon'))
            db.execute(
                "INSERT INTO mp_account_health (org_id,platform,score,level,metrics_json,alerts_json) "
                "VALUES (?,?,?,?,?,?)",
                (org_id, 'amazon', 92, 'Excellent',
                 _j.dumps({'order_defect_rate': 0.3, 'late_shipment_rate': 1.2, 'cancel_rate': 0.8}),
                 _j.dumps([])))
        except Exception:
            pass
        try:
            total_returns = int(order_count * 0.04)
            return_rate = round(total_returns / max(order_count, 1) * 100, 2)
            refunded_rev = round(total_returns * 185.50, 2)
            db.execute("DELETE FROM mp_returns WHERE org_id=? AND platform=?", (org_id, 'amazon'))
            db.execute(
                "INSERT INTO mp_returns (org_id,platform,total_orders,total_returns,return_rate,refunded_revenue,trend) "
                "VALUES (?,?,?,?,?,?,?)",
                (org_id, 'amazon', order_count, total_returns, return_rate, refunded_rev, 'stable'))
        except Exception:
            pass
        db.execute("UPDATE api_integrations SET last_sync=datetime('now'), status='connected' WHERE org_id=? AND platform='amazon'", (org_id,))
        db.commit()
        db.close()
        try:
            from sync_base import log_sync
            log_sync(org_id, 'amazon', 'full', 'success', records_synced=order_count + prod_count)
        except Exception:
            pass
        return jsonify({'ok': True, 'products_inserted': prod_count, 'orders_inserted': order_count,
                        'message': f'Simulacao Amazon: {prod_count} produtos, {order_count} pedidos (8 meses)'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e), 'trace': _tb.format_exc()[:800]})


# ── Shopee OAuth ─────────────────────────────────────────────────────────────

@app.route('/api/shopee/auth')
@login_required
def shopee_auth():
    """Redirect seller to Shopee authorization page."""
    from shopee_api import get_auth_url
    url = get_auth_url()
    return redirect(url)


@app.route('/api/server-ip')
def server_ip():
    """Return the public outbound IP of this server (for Shopee IP whitelist)."""
    import urllib.request as _ur
    services = [
        'https://api.ipify.org',
        'https://icanhazip.com',
        'https://ifconfig.me/ip',
        'https://checkip.amazonaws.com',
    ]
    ips = {}
    for svc in services:
        try:
            req = _ur.Request(svc, headers={'User-Agent': 'curl/7.68.0'})
            with _ur.urlopen(req, timeout=5) as r:
                ips[svc] = r.read().decode().strip()
        except Exception as e:
            ips[svc] = f'error: {e}'

    html = f'''<!DOCTYPE html>
<html><head><title>Server Outbound IP</title>
<style>
body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 40px auto; padding: 20px; background: #f5f5f5; }}
h1 {{ color: #ee4d2d; }}
.card {{ background: white; border-radius: 12px; padding: 24px; margin: 20px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
.big-ip {{ font-size: 32px; font-weight: 900; color: #ee4d2d; font-family: 'Courier New', monospace; text-align: center; padding: 20px; background: #fff3cd; border-radius: 8px; margin: 15px 0; }}
code {{ background: #eee; padding: 4px 8px; border-radius: 4px; }}
table {{ width: 100%; border-collapse: collapse; }}
td {{ padding: 8px; border-bottom: 1px solid #eee; }}
</style></head><body>
<h1>🌐 Railway Server Outbound IP</h1>
<div class="card">
  <p>Este e o IP publico que o Railway usa para fazer chamadas para outros servicos (como a API da Shopee).</p>
  <p><b>Use este IP no campo "APP IP Address Management" do Shopee Open Platform.</b></p>
</div>
<div class="card">
  <h2>IPs detectados:</h2>
  <table>'''
    for svc, ip in ips.items():
        html += f'<tr><td><code>{svc}</code></td><td><b>{ip}</b></td></tr>'

    # Pick the most common IP
    valid_ips = [v for v in ips.values() if not v.startswith('error')]
    if valid_ips:
        most_common = max(set(valid_ips), key=valid_ips.count)
        html += f'''</table>
</div>
<div class="card">
  <h2>✅ IP para usar no whitelist:</h2>
  <div class="big-ip">{most_common}</div>
  <p style="color:#856404;background:#fff3cd;padding:12px;border-radius:6px;">
    ⚠️ <b>Atencao:</b> O Railway usa IPs dinamicos que podem mudar ao reiniciar ou fazer deploy.
    Se a API da Shopee comecar a falhar no futuro, volte aqui para pegar o novo IP e atualizar no whitelist.
  </p>
</div>'''
    else:
        html += '</table></div><div class="card"><p>Nao foi possivel detectar o IP. Tente novamente.</p></div>'

    html += '</body></html>'
    return html, 200, {'Content-Type': 'text/html'}


@app.route('/api/shopee/auth-debug')
@login_required
def shopee_auth_debug():
    """Debug: show auth URL components to diagnose sign issues."""
    import hmac as _hmac, hashlib as _hashlib, time as _time
    from shopee_api import PARTNER_ID, PARTNER_KEY, REDIRECT_URL
    import urllib.parse as _up

    path = '/api/v2/shop/auth_partner'
    ts = int(_time.time())
    redirect_url = REDIRECT_URL

    SANDBOX_HOST = 'https://partner.test-stable.shopeemobile.com'
    PROD_HOST = 'https://partner.shopeemobile.com'

    # Key variants
    key_full = PARTNER_KEY
    key_no_prefix = PARTNER_KEY[4:] if PARTNER_KEY.startswith('shpk') else PARTNER_KEY
    try:
        key_hex_bytes = bytes.fromhex(key_no_prefix)
    except Exception:
        key_hex_bytes = None

    base_str = f'{PARTNER_ID}{path}{ts}'

    def compute_sign(key_bytes):
        return _hmac.new(key_bytes, base_str.encode('utf-8'), _hashlib.sha256).hexdigest()

    sign_full = compute_sign(key_full.encode('utf-8'))
    sign_nopfx = compute_sign(key_no_prefix.encode('utf-8'))
    sign_hex = compute_sign(key_hex_bytes) if key_hex_bytes else 'error'

    def build_url(host, sign):
        params = {'partner_id': PARTNER_ID, 'timestamp': ts, 'sign': sign, 'redirect': redirect_url}
        return f'{host}{path}?{_up.urlencode(params)}'

    tests = [
        ('1', 'PRODUCAO + Chave completa (shpk...)', 'Host de producao com chave inteira como UTF-8', PROD_HOST, sign_full, '#28a745'),
        ('2', 'PRODUCAO + Sem prefixo shpk', 'Host de producao, remove shpk, usa resto como UTF-8', PROD_HOST, sign_nopfx, '#17a2b8'),
        ('3', 'PRODUCAO + Hex-decoded', 'Host de producao, remove shpk, hex-decode em bytes', PROD_HOST, sign_hex, '#6f42c1'),
        ('4', 'SANDBOX + Chave completa (shpk...)', 'Host sandbox com chave inteira como UTF-8', SANDBOX_HOST, sign_full, '#ee4d2d'),
        ('5', 'SANDBOX + Sem prefixo shpk', 'Host sandbox, remove shpk, usa resto como UTF-8', SANDBOX_HOST, sign_nopfx, '#fd7e14'),
        ('6', 'SANDBOX + Hex-decoded', 'Host sandbox, remove shpk, hex-decode em bytes', SANDBOX_HOST, sign_hex, '#e83e8c'),
    ]

    cards_html = ''
    for num, title, desc, host, sign, color in tests:
        url = build_url(host, sign)
        cards_html += f'''
<div class="card">
  <h2 style="color:{color}">Teste {num} — {title}</h2>
  <p class="info">{desc}</p>
  <p><b>Host:</b> <code>{host}</code></p>
  <p><b>Sign:</b> <code>{sign[:20]}...</code></p>
  <a class="btn" style="background:{color}" href="{url}" target="_blank">🔗 Testar #{num}</a>
</div>'''

    html = f'''<!DOCTYPE html>
<html><head><title>Shopee Auth Debug</title>
<style>
body {{ font-family: Arial, sans-serif; max-width: 900px; margin: 40px auto; padding: 20px; background: #f5f5f5; }}
h1 {{ color: #ee4d2d; }}
.card {{ background: white; border-radius: 12px; padding: 20px; margin: 20px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
.card h2 {{ margin-top: 0; }}
a.btn {{ display: inline-block; padding: 12px 24px; color: white; text-decoration: none; border-radius: 8px; font-weight: bold; margin: 8px 0; font-size: 16px; }}
a.btn:hover {{ opacity: 0.85; }}
code {{ background: #eee; padding: 2px 6px; border-radius: 4px; font-size: 13px; word-break: break-all; }}
.info {{ color: #666; font-size: 14px; }}
.highlight {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 12px; margin: 10px 0; border-radius: 4px; }}
</style></head><body>
<h1>🔧 Shopee Auth Debug — 6 Testes</h1>

<div class="card">
  <h2>Configuracao</h2>
  <p><b>Partner ID:</b> <code>{PARTNER_ID}</code></p>
  <p><b>Key Length:</b> <code>{len(PARTNER_KEY)}</code></p>
  <p><b>Key Prefix:</b> <code>{PARTNER_KEY[:8]}...</code></p>
  <p><b>Timestamp:</b> <code>{ts}</code></p>
  <p><b>Redirect:</b> <code>{redirect_url}</code></p>
  <p><b>Base String:</b> <code>{base_str[:40]}...</code></p>
  <div class="highlight">
    <b>⚠️ Problema provavel:</b> Suas credenciais sao de <b>producao</b>, mas estavamos usando o host de <b>sandbox</b>.
    Os testes 1-3 usam o host de producao. Se um deles funcionar, confirma o diagnostico!
  </div>
</div>

{cards_html}

<div class="card">
  <h2>📋 Instrucoes</h2>
  <p>Clique em cada botao. Se abrir a <b>pagina de login da Shopee</b> = ✅ correto</p>
  <p>Se aparecer <b>"Wrong sign"</b> ou erro = ❌ errado</p>
  <p><b>Comece pelo Teste 1</b> (mais provavel de funcionar).</p>
</div>
</body></html>'''
    return html, 200, {'Content-Type': 'text/html'}


@app.route('/api/shopee/server-test')
@login_required
def shopee_server_test():
    """Server-side test: make direct HTTP requests to Shopee API to debug sign issues."""
    import hmac as _hmac, hashlib as _hashlib, time as _time
    from shopee_api import PARTNER_ID, PARTNER_KEY, REDIRECT_URL
    import urllib.parse as _up
    import urllib.request as _ur
    import urllib.error as _ue

    SANDBOX = 'https://partner.test-stable.shopeemobile.com'
    path = '/api/v2/shop/auth_partner'
    ts = int(_time.time())

    results = []

    # Test multiple approaches
    key_full = PARTNER_KEY
    key_nopfx = PARTNER_KEY[4:] if PARTNER_KEY.startswith('shpk') else PARTNER_KEY
    try:
        key_hex = bytes.fromhex(key_nopfx)
    except Exception:
        key_hex = None

    # Also try: what if base_string needs redirect?
    base_normal = f'{PARTNER_ID}{path}{ts}'
    base_with_redirect = f'{PARTNER_ID}{path}{ts}{REDIRECT_URL}'

    test_cases = [
        ('Chave completa + base normal', key_full.encode(), base_normal),
        ('Sem shpk + base normal', key_nopfx.encode(), base_normal),
        ('Hex-decoded + base normal', key_hex, base_normal),
        ('Chave completa + base com redirect', key_full.encode(), base_with_redirect),
        ('Sem shpk + base com redirect', key_nopfx.encode(), base_with_redirect),
        ('Hex-decoded + base com redirect', key_hex, base_with_redirect),
    ]

    for label, key_bytes, base_str in test_cases:
        if key_bytes is None:
            results.append({'test': label, 'error': 'key_bytes is None'})
            continue
        sign = _hmac.new(key_bytes, base_str.encode(), _hashlib.sha256).hexdigest()
        params = {'partner_id': PARTNER_ID, 'timestamp': ts, 'sign': sign, 'redirect': REDIRECT_URL}
        url = f'{SANDBOX}{path}?{_up.urlencode(params)}'
        try:
            req = _ur.Request(url, headers={'User-Agent': 'Sellvance/1.0'})
            with _ur.urlopen(req, timeout=10) as resp:
                body = resp.read().decode()
                results.append({
                    'test': label,
                    'status': resp.status,
                    'base_string': base_str[:60] + '...',
                    'sign': sign[:16] + '...',
                    'response': body[:500],
                    'result': '✅ SUCCESS' if resp.status == 200 else f'⚠️ {resp.status}'
                })
        except _ue.HTTPError as e:
            body = e.read().decode() if hasattr(e, 'read') else str(e)
            results.append({
                'test': label,
                'status': e.code,
                'base_string': base_str[:60] + '...',
                'sign': sign[:16] + '...',
                'response': body[:500],
                'result': f'❌ HTTP {e.code}'
            })
        except Exception as e:
            results.append({
                'test': label,
                'error': str(e)[:200],
                'base_string': base_str[:60] + '...',
                'sign': sign[:16] + '...',
                'result': f'❌ {type(e).__name__}'
            })

    # Build HTML
    cards = ''
    for i, r in enumerate(results, 1):
        color = '#28a745' if '✅' in r.get('result', '') else '#dc3545'
        resp_text = r.get('response', r.get('error', 'N/A'))
        cards += f'''
<div class="card">
  <h3 style="color:{color}">Teste {i}: {r['test']}</h3>
  <p><b>Resultado:</b> {r.get('result','?')}</p>
  <p><b>Base string:</b> <code>{r.get('base_string','?')}</code></p>
  <p><b>Sign:</b> <code>{r.get('sign','?')}</code></p>
  <p><b>Resposta Shopee:</b></p>
  <pre style="background:#f8f9fa;padding:10px;border-radius:6px;overflow-x:auto;font-size:12px">{resp_text}</pre>
</div>'''

    # Also show server time info
    import datetime as _dt
    server_time = _dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')

    html = f'''<!DOCTYPE html>
<html><head><title>Shopee Server Test</title>
<style>
body {{ font-family: Arial, sans-serif; max-width: 900px; margin: 40px auto; padding: 20px; background: #f5f5f5; }}
h1 {{ color: #ee4d2d; }}
.card {{ background: white; border-radius: 12px; padding: 20px; margin: 15px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
code {{ background: #eee; padding: 2px 6px; border-radius: 4px; font-size: 12px; word-break: break-all; }}
pre {{ white-space: pre-wrap; word-break: break-all; }}
.info {{ background: #e8f4fd; padding: 15px; border-radius: 8px; margin: 15px 0; }}
</style></head><body>
<h1>🔬 Shopee Server-Side Test</h1>
<div class="info">
  <p><b>Server Time:</b> {server_time}</p>
  <p><b>Timestamp usado:</b> {ts}</p>
  <p><b>Partner ID:</b> {PARTNER_ID}</p>
  <p><b>Key (primeiros 12):</b> {PARTNER_KEY[:12]}...</p>
  <p><b>Key length:</b> {len(PARTNER_KEY)}</p>
  <p><b>Redirect:</b> {REDIRECT_URL}</p>
  <p><b>Host:</b> {SANDBOX}</p>
  <p>Estes testes sao feitos <b>direto do servidor Railway</b>, nao pelo browser.</p>
</div>
{cards}
</body></html>'''
    return html, 200, {'Content-Type': 'text/html'}


@app.route('/api/shopee/callback')
def shopee_callback():
    """Handle OAuth callback from Shopee."""
    code = request.args.get('code', '')
    shop_id = request.args.get('shop_id', '')

    if not code or not shop_id:
        return jsonify({'ok': False, 'error': 'Missing code or shop_id',
                        'params': dict(request.args)}), 400

    from shopee_api import get_access_token, get_shop_info
    # Exchange code for access token
    token_resp = get_access_token(code, shop_id)
    if 'error' in token_resp and not token_resp.get('access_token'):
        return jsonify({'ok': False, 'error': 'Token exchange failed', 'details': token_resp}), 400

    access_token = token_resp.get('access_token', '')
    refresh_token = token_resp.get('refresh_token', '')
    expire_in = token_resp.get('expire_in', 14400)
    import time as _time_mod
    expire_ts = int(_time_mod.time()) + expire_in

    # Get shop info
    shop_info = get_shop_info(access_token, int(shop_id))
    shop_name = ''
    if shop_info.get('response'):
        shop_name = shop_info['response'].get('shop_name', f'Shop {shop_id}')

    # Save to DB
    org_id = session.get('org_id', 1)
    db = get_db()
    creds = {
        'shop_id': int(shop_id),
        'access_token': access_token,
        'refresh_token': refresh_token,
        'expire_in_ts': expire_ts,
    }
    existing = db.execute("SELECT id FROM api_integrations WHERE org_id=? AND platform='shopee'",
                          (org_id,)).fetchone()
    if existing:
        db.execute("""UPDATE api_integrations SET status='connected', account_id=?, account_name=?,
                      credentials_json=?, last_sync=datetime('now') WHERE id=?""",
                   (str(shop_id), shop_name, json.dumps(creds), existing['id']))
    else:
        db.execute("""INSERT INTO api_integrations (org_id, platform, status, account_id, account_name, credentials_json)
                      VALUES (?,?,?,?,?,?)""",
                   (org_id, 'shopee', 'connected', str(shop_id), shop_name, json.dumps(creds)))
    db.commit()
    db.close()

    return redirect('/integrations?shopee=connected')


@app.route('/api/shopee/sync')
@login_required
def shopee_sync():
    """Trigger manual sync of Shopee data."""
    org_id = session.get('org_id', 1)
    try:
        from shopee_api import sync_all
        result = sync_all(org_id)
        return jsonify({'ok': True, **result})
    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()[:500]})


@app.route('/api/shopee/debug')
@login_required
def shopee_debug():
    """Debug Shopee connection status."""
    org_id = session.get('org_id', 1)
    db = get_db()
    row = db.execute("SELECT * FROM api_integrations WHERE org_id=? AND platform='shopee'", (org_id,)).fetchone()
    db.close()
    if not row:
        return jsonify({'status': 'not_connected'})
    info = dict(row)
    creds = json.loads(info.get('credentials_json', '{}'))
    # Don't expose full tokens
    safe_creds = {
        'shop_id': creds.get('shop_id'),
        'has_access_token': bool(creds.get('access_token')),
        'has_refresh_token': bool(creds.get('refresh_token')),
        'expire_ts': creds.get('expire_in_ts'),
    }
    info['credentials_json'] = safe_creds
    return jsonify(info)


@app.route('/api/simulate/shopee')
def simulate_shopee_data():
    """Insert realistic Shopee test data to verify the full pipeline works."""
    import random, json as _j, traceback as _tb
    from datetime import datetime, timedelta
    from database import get_db
    try:
        org_id = 1
        db = get_db()
        # Reconectar Shopee no banco
        existing = db.execute("SELECT id FROM api_integrations WHERE org_id=? AND platform='shopee'", (org_id,)).fetchone()
        if existing:
            db.execute("UPDATE api_integrations SET status='connected', last_sync=datetime('now') WHERE org_id=? AND platform='shopee'", (org_id,))
        else:
            db.execute("INSERT INTO api_integrations (org_id,platform,status,account_id,account_name,config_json,last_sync) VALUES (?,?,?,?,?,?,datetime('now'))",
                       (org_id, 'shopee', 'connected', 'shopee_br_001', 'Loja Shopee BR', _j.dumps({'shop_id': 'shopee_br_001', 'simulated': True})))
        # Limpar dados anteriores
        db.execute("DELETE FROM orders WHERE org_id=? AND marketplace='shopee'", (org_id,))
        db.execute("DELETE FROM mp_products WHERE org_id=? AND platform='shopee'", (org_id,))
        db.execute("DELETE FROM mp_account_health WHERE org_id=? AND platform='shopee'", (org_id,))
        db.execute("DELETE FROM mp_returns WHERE org_id=? AND platform='shopee'", (org_id,))
        # Produtos Shopee realistas (BR)
        products = [
            ('SHP001', 'Capa Celular Samsung Galaxy S24 Ultra Silicone', 29.90, 320),
            ('SHP002', 'Pelicula Vidro Temperado iPhone 15 Pro Max', 14.90, 580),
            ('SHP003', 'Fone de Ouvido Bluetooth i12 TWS', 39.90, 245),
            ('SHP004', 'Carregador Turbo USB-C 25W Samsung', 49.90, 167),
            ('SHP005', 'Cabo USB-C para USB-C 2m Nylon', 19.90, 412),
            ('SHP006', 'Suporte Celular Veicular Magnetico', 24.90, 198),
            ('SHP007', 'Mini Caixa de Som Bluetooth Portatil', 59.90, 134),
            ('SHP008', 'Relogio Smartband Monitor Cardiaco', 79.90, 89),
            ('SHP009', 'Luminaria LED Mesa USB Touch 3 Cores', 44.90, 156),
            ('SHP010', 'Mouse Sem Fio Silencioso 2.4GHz', 34.90, 267),
            ('SHP011', 'Hub Adaptador USB-C 4 Portas', 39.90, 143),
            ('SHP012', 'Ring Light 6pol com Tripe Celular', 54.90, 201),
            ('SHP013', 'Fita LED RGB 5m Controle Remoto', 34.90, 178),
            ('SHP014', 'Organizador Cabos Silicone 6 Vias', 12.90, 890),
            ('SHP015', 'Mousepad Gamer Grande 70x30cm RGB', 49.90, 112),
        ]
        prod_count = 0
        for ext_id, title, price, stock in products:
            try:
                db.execute("INSERT INTO mp_products (org_id,platform,external_id,title,price,stock_qty,status) VALUES (?,?,?,?,?,?,?)",
                           (org_id, 'shopee', ext_id, title, price, stock, 'active'))
                prod_count += 1
            except Exception:
                pass
        # Pedidos 8 meses - Shopee tem volume maior, ticket menor
        statuses_pool = ['delivered'] * 70 + ['pending'] * 12 + ['shipped'] * 12 + ['cancelled'] * 6
        now = datetime.now()
        order_rows = []
        for day_offset in range(240):
            date = now - timedelta(days=day_offset)
            n_orders = random.randint(5, 15)
            if date.weekday() in (4, 5):
                n_orders += random.randint(3, 8)
            # Datas promocionais Shopee (dia do mes = mes, ex: 3.3, 4.4, etc)
            if date.day == date.month and date.month <= 12:
                n_orders += random.randint(15, 30)
            for i in range(n_orders):
                prod = random.choice(products)
                _, title, price, _ = prod
                qty = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
                gmv = round(price * qty, 2)
                # Shopee tem taxas diferentes
                revenue = round(gmv * random.uniform(0.78, 0.92), 2)
                cost = round(gmv * random.uniform(0.30, 0.45), 2)
                status = random.choice(statuses_pool) if day_offset > 3 else random.choice(['pending', 'shipped'])
                order_date = date.replace(hour=random.randint(6, 23), minute=random.randint(0, 59), second=random.randint(0, 59))
                ext_id = f"SH{date.strftime('%y%m%d')}{random.randint(100000,999999)}"
                order_rows.append((org_id, None, 'shopee', ext_id, status, gmv, revenue, cost, 'organic',
                                   order_date.strftime('%Y-%m-%d %H:%M:%S')))
        db.executemany("INSERT OR REPLACE INTO orders (org_id,contact_id,marketplace,external_id,status,gmv,revenue,cost,channel,ordered_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                       order_rows)
        order_count = len(order_rows)
        # Account Health
        db.execute("INSERT INTO mp_account_health (org_id,platform,score,level,metrics_json,alerts_json) VALUES (?,?,?,?,?,?)",
                   (org_id, 'shopee', 88, 'Good',
                    _j.dumps({'penalty_points': 1, 'chat_response_rate': 96.5, 'late_shipment_rate': 2.1, 'cancel_rate': 1.5}),
                    _j.dumps([{'type': 'warning', 'message': 'Taxa de resposta no chat abaixo de 98%'}])))
        # Returns
        total_returns = int(order_count * 0.05)
        return_rate = round(total_returns / max(order_count, 1) * 100, 2)
        refunded_rev = round(total_returns * 42.50, 2)
        db.execute("INSERT INTO mp_returns (org_id,platform,total_orders,total_returns,return_rate,refunded_revenue,trend) VALUES (?,?,?,?,?,?,?)",
                   (org_id, 'shopee', order_count, total_returns, return_rate, refunded_rev, 'up'))
        db.commit()
        db.close()
        try:
            from sync_base import log_sync
            log_sync(org_id, 'shopee', 'full', 'success', records_synced=order_count + prod_count)
        except Exception:
            pass
        return jsonify({'ok': True, 'products_inserted': prod_count, 'orders_inserted': order_count,
                        'message': f'Simulacao Shopee: {prod_count} produtos, {order_count} pedidos (8 meses)'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e), 'trace': _tb.format_exc()[:800]})


@app.route('/api/fix/amazon-secret')
def fix_amazon_secret():
    """One-time fix: update Amazon client_secret and reset status."""
    import json as _j
    from database import get_db
    org_id = 1
    db = get_db()
    row = db.execute('SELECT config_json FROM api_integrations WHERE org_id=? AND platform=?', (org_id, 'amazon')).fetchone()
    if not row:
        return jsonify({'error': 'No Amazon integration found'})
    cfg = _j.loads(row['config_json'] or '{}')
    old_secret = cfg.get('client_secret', '')
    cfg['client_secret'] = 'amzn1.oa2-cs.v1.9b1134d563deac59e453ed9efbbc9b1ff4cc762f2d537d574d0f42f12b8749cc'
    db.execute("UPDATE api_integrations SET config_json=?, status='connected' WHERE org_id=? AND platform=?",
               (_j.dumps(cfg), org_id, 'amazon'))
    db.commit()
    db.close()
    return jsonify({'ok': True, 'old_secret_suffix': old_secret[-10:], 'new_secret_suffix': cfg['client_secret'][-10:]})


@app.route('/api/debug/amazon')
def debug_amazon():
    """Step-by-step Amazon SP-API diagnostic — shows exactly where sync fails."""
    import traceback, os, json as _json, io, sys
    org_id = 1
    steps = []

    # Step 1: Load credentials
    try:
        from oauth_manager import get_integration
        integ = get_integration(org_id, 'amazon')
        if not integ:
            return jsonify({'error': 'No Amazon integration found', 'steps': steps})
        cfg = integ.get('config', {})
        cred_info = {
            'status': integ.get('status'),
            'has_client_id': bool(cfg.get('client_id')),
            'client_id_prefix': (cfg.get('client_id', '')[:30] + '…') if cfg.get('client_id') else None,
            'has_client_secret': bool(cfg.get('client_secret')),
            'has_refresh_token': bool(cfg.get('refresh_token')),
            'refresh_token_prefix': (cfg.get('refresh_token', '')[:15] + '…') if cfg.get('refresh_token') else None,
            'seller_id': cfg.get('seller_id', ''),
            'marketplace_id': cfg.get('marketplace_id', ''),
            'has_aws_key': bool(os.environ.get('AMAZON_AWS_ACCESS_KEY')),
            'aws_key_prefix': (os.environ.get('AMAZON_AWS_ACCESS_KEY', '')[:6] + '…'),
            'has_aws_secret': bool(os.environ.get('AMAZON_AWS_SECRET_KEY')),
        }
        steps.append({'step': '1_credentials', 'ok': True, 'data': cred_info})
    except Exception as e:
        steps.append({'step': '1_credentials', 'ok': False, 'error': str(e)})
        return jsonify({'steps': steps})

    # Step 2: LWA token exchange
    try:
        from sync_amazon import _get_lwa_token, _ENDPOINT, _DEFAULT_ENDPOINT
        client_id = cfg.get('client_id', '')
        client_secret = cfg.get('client_secret', '')
        refresh_token = cfg.get('refresh_token', '')
        access_token, expires_in = _get_lwa_token(client_id, client_secret, refresh_token)
        steps.append({
            'step': '2_lwa_token',
            'ok': True,
            'token_prefix': access_token[:20] + '…',
            'expires_in': expires_in,
        })
    except Exception as e:
        steps.append({'step': '2_lwa_token', 'ok': False, 'error': str(e), 'trace': traceback.format_exc()[:400]})
        return jsonify({'steps': steps})

    # Step 3: Test Orders API
    try:
        from sync_amazon import _sp_get
        import urllib.parse, urllib.request, urllib.error
        mp_id = cfg.get('marketplace_id', 'A2Q3Y263D00KWC')
        endpoint = _ENDPOINT.get(mp_id, _DEFAULT_ENDPOINT)
        aws_key = os.environ.get('AMAZON_AWS_ACCESS_KEY', '')
        aws_secret = os.environ.get('AMAZON_AWS_SECRET_KEY', '')

        from datetime import datetime, timedelta, timezone
        since = (datetime.now(timezone.utc) - timedelta(days=90)).strftime('%Y-%m-%dT%H:%M:%SZ')
        params = {
            'MarketplaceIds': mp_id,
            'CreatedAfter': since,
            'MaxResultsPerPage': '10',
        }
        resp = _sp_get(endpoint, '/orders/v0/orders', access_token,
                       params=params, aws_key=aws_key, aws_secret=aws_secret, region='us-east-1')
        orders_payload = resp.get('payload', {})
        orders_list = orders_payload.get('Orders', [])
        steps.append({
            'step': '3_orders_api',
            'ok': True,
            'endpoint': endpoint,
            'params': params,
            'response_keys': list(resp.keys()) if isinstance(resp, dict) else 'not_dict',
            'payload_keys': list(orders_payload.keys()) if orders_payload else 'empty',
            'orders_count': len(orders_list),
            'first_order': {k: v for k, v in orders_list[0].items() if k in ('AmazonOrderId', 'OrderStatus', 'PurchaseDate', 'OrderTotal')} if orders_list else None,
            'raw_response_preview': str(resp)[:500],
        })
    except Exception as e:
        steps.append({'step': '3_orders_api', 'ok': False, 'error': str(e), 'trace': traceback.format_exc()[:400]})

    # Step 4: Test Listings API
    try:
        seller_id = cfg.get('seller_id', '')
        resp2 = _sp_get(endpoint, f'/listings/2021-08-01/items/{seller_id}', access_token,
                        params={'marketplaceIds': mp_id, 'includedData': 'summaries', 'pageSize': '5'},
                        aws_key=aws_key, aws_secret=aws_secret, region='us-east-1')
        steps.append({
            'step': '4_listings_api',
            'ok': True,
            'response_keys': list(resp2.keys()) if isinstance(resp2, dict) else 'not_dict',
            'items_count': len(resp2.get('items', [])),
            'raw_response_preview': str(resp2)[:500],
        })
    except Exception as e:
        steps.append({'step': '4_listings_api', 'ok': False, 'error': str(e), 'trace': traceback.format_exc()[:400]})

    # Step 5: Raw HTTP test (bypass _sp_get to see actual error)
    try:
        from sync_amazon import _sigv4_headers
        qs = urllib.parse.urlencode({'MarketplaceIds': mp_id, 'CreatedAfter': since, 'MaxResultsPerPage': '5'})
        url = f"https://{endpoint}/orders/v0/orders?{qs}"
        base_hdrs = {'x-amz-access-token': access_token, 'Content-Type': 'application/json'}
        signed_hdrs = _sigv4_headers('GET', url, base_hdrs, b'', aws_key, aws_secret, 'us-east-1')
        req = urllib.request.Request(url, headers=signed_hdrs, method='GET')
        with urllib.request.urlopen(req, timeout=20) as raw_resp:
            raw_body = raw_resp.read().decode('utf-8')[:800]
            raw_status = raw_resp.status
        steps.append({'step': '5_raw_http', 'ok': True, 'status': raw_status, 'body': raw_body})
    except urllib.error.HTTPError as e:
        body_err = e.read().decode()[:600]
        steps.append({'step': '5_raw_http', 'ok': False, 'http_code': e.code, 'body': body_err})
    except Exception as e:
        steps.append({'step': '5_raw_http', 'ok': False, 'error': str(e), 'trace': traceback.format_exc()[:400]})

    return jsonify({'steps': steps})


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

# ── CRM Automacao ────────────────────────────────────────────────────────────

@app.route('/automacao')
@login_required
@plan_required('crm')
def crm_automacao():
    """Dashboard de automacoes CRM."""
    org_id = session.get('org_id', 1)
    db = get_db()
    automations = [dict(r) for r in db.execute(
        'SELECT * FROM crm_automations WHERE org_id=? ORDER BY executions DESC', (org_id,)).fetchall()]
    # Totais
    total_exec = sum(a['executions'] for a in automations)
    total_conv = sum(a['conversions'] for a in automations)
    total_rev = sum(a['revenue'] for a in automations)
    active_count = sum(1 for a in automations if a['is_active'])
    # Campanhas WA e Email recentes
    wa_camps = [dict(r) for r in db.execute(
        'SELECT * FROM whatsapp_campaigns WHERE org_id=? ORDER BY id DESC LIMIT 5', (org_id,)).fetchall()]
    email_camps = [dict(r) for r in db.execute(
        'SELECT * FROM email_campaigns WHERE org_id=? ORDER BY id DESC LIMIT 5', (org_id,)).fetchall()]
    # Contatos por segmento
    segments = {}
    for row in db.execute(
        'SELECT rfm_segment, COUNT(*) as cnt FROM contacts WHERE org_id=? GROUP BY rfm_segment', (org_id,)).fetchall():
        segments[row['rfm_segment']] = row['cnt']
    db.close()
    return render_template('crm_automacao.html', page='automacao',
                           automations=automations, wa_camps=wa_camps, email_camps=email_camps,
                           segments=segments, total_exec=total_exec, total_conv=total_conv,
                           total_rev=total_rev, active_count=active_count)


@app.route('/api/automacao/toggle/<int:auto_id>', methods=['POST'])
@login_required
def api_toggle_automation(auto_id):
    org_id = session.get('org_id', 1)
    db = get_db()
    row = db.execute('SELECT is_active FROM crm_automations WHERE id=? AND org_id=?', (auto_id, org_id)).fetchone()
    if not row:
        db.close()
        return jsonify({'ok': False}), 404
    new_status = 0 if row['is_active'] else 1
    db.execute('UPDATE crm_automations SET is_active=? WHERE id=? AND org_id=?', (new_status, auto_id, org_id))
    db.commit()
    db.close()
    return jsonify({'ok': True, 'is_active': new_status})


@app.route('/api/automacao/create', methods=['POST'])
@login_required
def api_create_automation():
    org_id = session.get('org_id', 1)
    data = request.get_json(silent=True) or {}
    name = data.get('name', '').strip()
    trigger_type = data.get('trigger_type', 'segment_enter')
    segment = data.get('segment', 'all')
    channel = data.get('channel', 'whatsapp')
    message = data.get('message_template', '').strip()
    subject = data.get('subject', '').strip()
    delay = int(data.get('delay_hours', 0))
    if not name or not message:
        return jsonify({'ok': False, 'error': 'Nome e mensagem obrigatorios'}), 400
    db = get_db()
    db.execute(
        "INSERT INTO crm_automations (org_id,name,trigger_type,segment,channel,message_template,subject,delay_hours) VALUES (?,?,?,?,?,?,?,?)",
        (org_id, name, trigger_type, segment, channel, message, subject, delay))
    db.commit()
    db.close()
    return jsonify({'ok': True})


# ── Apresentacao (publica) ───────────────────────────────────────────────────

@app.route('/apresentacao')
def apresentacao():
    """Pagina de apresentacao publica do Sellvance."""
    return render_template('apresentacao.html')


# ── Logistica & Fulfillment ──────────────────────────────────────────────────

@app.route('/logistica')
@login_required
@plan_required('logistica')
def logistica():
    """Dashboard de logistica e rastreamento."""
    org_id = session.get('org_id', 1)
    db = get_db()
    shipments = [dict(r) for r in db.execute(
        'SELECT * FROM shipments WHERE org_id=? ORDER BY created_at DESC', (org_id,)).fetchall()]
    db.close()

    # KPIs
    total = len(shipments)
    delivered = [s for s in shipments if s['status'] == 'delivered']
    in_transit = [s for s in shipments if s['status'] == 'in_transit']
    pending = [s for s in shipments if s['status'] == 'pending']
    shipped = [s for s in shipments if s['status'] == 'shipped']
    returned = [s for s in shipments if s['status'] == 'returned']

    # Tempo medio de entrega (dias)
    from datetime import datetime as _dt
    delivery_days = []
    for s in delivered:
        if s['shipped_at'] and s['delivered_at']:
            try:
                d1 = _dt.strptime(s['shipped_at'], '%Y-%m-%d %H:%M:%S')
                d2 = _dt.strptime(s['delivered_at'], '%Y-%m-%d %H:%M:%S')
                delivery_days.append((d2 - d1).days)
            except Exception:
                pass
    avg_delivery = round(sum(delivery_days) / len(delivery_days), 1) if delivery_days else 0

    # Entregas por transportadora
    carrier_stats = {}
    for s in shipments:
        c = s['carrier'] or 'Sem transportadora'
        if c not in carrier_stats:
            carrier_stats[c] = {'total': 0, 'delivered': 0, 'in_transit': 0, 'returned': 0, 'days': []}
        carrier_stats[c]['total'] += 1
        if s['status'] == 'delivered':
            carrier_stats[c]['delivered'] += 1
            if s['shipped_at'] and s['delivered_at']:
                try:
                    d1 = _dt.strptime(s['shipped_at'], '%Y-%m-%d %H:%M:%S')
                    d2 = _dt.strptime(s['delivered_at'], '%Y-%m-%d %H:%M:%S')
                    carrier_stats[c]['days'].append((d2 - d1).days)
                except Exception:
                    pass
        elif s['status'] == 'in_transit':
            carrier_stats[c]['in_transit'] += 1
        elif s['status'] == 'returned':
            carrier_stats[c]['returned'] += 1
    for c in carrier_stats:
        days = carrier_stats[c]['days']
        carrier_stats[c]['avg_days'] = round(sum(days) / len(days), 1) if days else 0
        t = carrier_stats[c]['total']
        carrier_stats[c]['delivery_rate'] = round(carrier_stats[c]['delivered'] * 100 / t, 0) if t else 0

    # Entregas por estado
    state_counts = {}
    for s in shipments:
        st = s['dest_state'] or '??'
        state_counts[st] = state_counts.get(st, 0) + 1
    state_counts = dict(sorted(state_counts.items(), key=lambda x: -x[1]))

    # Atrasados (in_transit alem da estimativa)
    late_shipments = []
    now_str = _dt.now().strftime('%Y-%m-%d %H:%M:%S')
    for s in in_transit + shipped:
        if s.get('estimated_delivery') and s['estimated_delivery'] < now_str:
            late_shipments.append(s)

    return render_template('logistica.html', page='logistica',
                           shipments=shipments, total=total,
                           delivered_count=len(delivered), in_transit_count=len(in_transit),
                           pending_count=len(pending), shipped_count=len(shipped),
                           returned_count=len(returned), avg_delivery=avg_delivery,
                           carrier_stats=carrier_stats, state_counts=state_counts,
                           late_count=len(late_shipments), late_shipments=late_shipments)


# ── Mini Loja Virtual ────────────────────────────────────────────────────────

import re as _re_mod, unicodedata as _unicodedata

def _slugify(text):
    text = _unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    text = _re_mod.sub(r'[^\w\s-]', '', text.lower())
    return _re_mod.sub(r'[-\s]+', '-', text).strip('-')


def _ensure_loja_config(org_id):
    """Garante que existe config da Mini Loja para o org."""
    db = get_db()
    row = db.execute('SELECT * FROM mini_loja_config WHERE org_id=?', (org_id,)).fetchone()
    if row:
        db.close()
        return dict(row)
    # Criar config padrao
    org = db.execute('SELECT name FROM organizations WHERE id=?', (org_id,)).fetchone()
    name = org['name'] if org else f'Loja {org_id}'
    slug = _slugify(name)
    # Garantir slug unico
    base_slug = slug
    counter = 1
    while db.execute('SELECT id FROM mini_loja_config WHERE slug=?', (slug,)).fetchone():
        slug = f'{base_slug}-{counter}'
        counter += 1
    db.execute(
        "INSERT INTO mini_loja_config (org_id,slug,store_name) VALUES (?,?,?)",
        (org_id, slug, name))
    db.commit()
    row = db.execute('SELECT * FROM mini_loja_config WHERE org_id=?', (org_id,)).fetchone()
    db.close()
    return dict(row)


@app.route('/loja/<slug>')
def mini_loja_public(slug):
    """Vitrine publica da Mini Loja — sem login."""
    import hashlib
    db = get_db()
    config = db.execute('SELECT * FROM mini_loja_config WHERE slug=? AND is_active=1', (slug,)).fetchone()
    if not config:
        db.close()
        return render_template('mini_loja_404.html'), 404
    config = dict(config)
    org_id = config['org_id']
    # Produtos visiveis
    products = db.execute('''
        SELECT mp.* FROM mp_products mp
        JOIN mini_loja_products mlp ON mp.id = mlp.mp_product_id
        WHERE mlp.org_id=? AND mlp.is_visible=1 AND mp.status='active'
        ORDER BY mlp.sort_order, mp.title
    ''', (org_id,)).fetchall()
    products = [dict(p) for p in products]
    # Analytics — page view
    try:
        ip_raw = request.remote_addr or '0.0.0.0'
        ip_hash = hashlib.sha256(ip_raw.encode()).hexdigest()[:16]
        db.execute("INSERT INTO mini_loja_analytics (org_id,event_type,ip_hash,referrer) VALUES (?,?,?,?)",
                   (org_id, 'page_view', ip_hash, request.referrer or ''))
        db.commit()
    except Exception:
        pass
    db.close()
    return render_template('mini_loja_public.html', config=config, products=products)


@app.route('/api/loja/click', methods=['POST'])
def mini_loja_click():
    """Registra clique no WhatsApp (chamado do JS da loja publica)."""
    import hashlib
    data = request.get_json(silent=True) or {}
    org_id = data.get('org_id')
    product_id = data.get('product_id')
    if not org_id:
        return jsonify({'ok': False}), 400
    try:
        db = get_db()
        ip_hash = hashlib.sha256((request.remote_addr or '').encode()).hexdigest()[:16]
        db.execute("INSERT INTO mini_loja_analytics (org_id,event_type,product_id,ip_hash) VALUES (?,?,?,?)",
                   (org_id, 'whatsapp_click', product_id, ip_hash))
        db.commit()
        db.close()
    except Exception:
        pass
    return jsonify({'ok': True})


@app.route('/mini-loja')
@login_required
@plan_required('mini-loja')
def mini_loja_admin():
    """Painel admin da Mini Loja."""
    org_id = session.get('org_id', 1)
    config = _ensure_loja_config(org_id)
    db = get_db()
    # Produtos com status de visibilidade
    all_products = db.execute('SELECT * FROM mp_products WHERE org_id=? AND status=?', (org_id, 'active')).fetchall()
    visible_ids = set()
    for r in db.execute('SELECT mp_product_id FROM mini_loja_products WHERE org_id=? AND is_visible=1', (org_id,)).fetchall():
        visible_ids.add(r['mp_product_id'])
    products = []
    for p in all_products:
        pd = dict(p)
        pd['loja_visible'] = pd['id'] in visible_ids
        products.append(pd)
    # Analytics ultimos 30 dias
    views_30d = db.execute(
        "SELECT COUNT(*) as c FROM mini_loja_analytics WHERE org_id=? AND event_type='page_view' AND created_at >= datetime('now','-30 days')",
        (org_id,)).fetchone()['c']
    clicks_30d = db.execute(
        "SELECT COUNT(*) as c FROM mini_loja_analytics WHERE org_id=? AND event_type='whatsapp_click' AND created_at >= datetime('now','-30 days')",
        (org_id,)).fetchone()['c']
    db.close()
    return render_template('mini_loja_admin.html', page='mini-loja', config=config,
                           products=products, views_30d=views_30d, clicks_30d=clicks_30d)


@app.route('/mini-loja/save', methods=['POST'])
@login_required
def mini_loja_save():
    org_id = session.get('org_id', 1)
    db = get_db()
    store_name = request.form.get('store_name', '').strip()
    slug = _slugify(request.form.get('slug', '').strip() or store_name)
    whatsapp = _re_mod.sub(r'\D', '', request.form.get('whatsapp', ''))
    logo_url = request.form.get('logo_url', '').strip()
    accent_color = request.form.get('accent_color', '#6c63ff').strip()
    banner_text = request.form.get('banner_text', '').strip()
    is_active = 1 if request.form.get('is_active') else 0
    # Validar slug unico
    existing = db.execute('SELECT org_id FROM mini_loja_config WHERE slug=? AND org_id!=?', (slug, org_id)).fetchone()
    if existing:
        slug = slug + '-' + str(org_id)
    db.execute('''UPDATE mini_loja_config SET store_name=?, slug=?, whatsapp=?, logo_url=?,
                  accent_color=?, banner_text=?, is_active=?, updated_at=datetime('now')
                  WHERE org_id=?''',
               (store_name, slug, whatsapp, logo_url, accent_color, banner_text, is_active, org_id))
    db.commit()
    db.close()
    return redirect('/mini-loja')


@app.route('/api/mini-loja/toggle-product', methods=['POST'])
@login_required
def mini_loja_toggle_product():
    org_id = session.get('org_id', 1)
    data = request.get_json(silent=True) or {}
    mp_id = data.get('mp_product_id')
    visible = 1 if data.get('visible', True) else 0
    if not mp_id:
        return jsonify({'ok': False}), 400
    db = get_db()
    existing = db.execute('SELECT id FROM mini_loja_products WHERE org_id=? AND mp_product_id=?', (org_id, mp_id)).fetchone()
    if existing:
        db.execute('UPDATE mini_loja_products SET is_visible=? WHERE org_id=? AND mp_product_id=?', (visible, org_id, mp_id))
    else:
        db.execute('INSERT INTO mini_loja_products (org_id,mp_product_id,is_visible) VALUES (?,?,?)', (org_id, mp_id, visible))
    db.commit()
    db.close()
    return jsonify({'ok': True, 'visible': visible})


@app.route('/api/mini-loja/add-all', methods=['POST'])
@login_required
def mini_loja_add_all():
    """Adiciona todos os produtos ativos na Mini Loja."""
    org_id = session.get('org_id', 1)
    db = get_db()
    products = db.execute('SELECT id FROM mp_products WHERE org_id=? AND status=?', (org_id, 'active')).fetchall()
    count = 0
    for p in products:
        try:
            db.execute('INSERT OR REPLACE INTO mini_loja_products (org_id,mp_product_id,is_visible) VALUES (?,?,1)', (org_id, p['id']))
            count += 1
        except Exception:
            pass
    db.commit()
    db.close()
    return jsonify({'ok': True, 'count': count})


# ── Link de Pagamento ────────────────────────────────────────────────────────

import string as _string_mod, random as _random_mod

def _gen_pay_code(length=8):
    return ''.join(_random_mod.choices(_string_mod.ascii_uppercase + _string_mod.digits, k=length))


@app.route('/pagamentos')
@login_required
@plan_required('pagamentos')
def pagamentos():
    """Dashboard de Links de Pagamento."""
    org_id = session.get('org_id', 1)
    db = get_db()
    links = [dict(r) for r in db.execute(
        'SELECT * FROM payment_links WHERE org_id=? ORDER BY created_at DESC', (org_id,)).fetchall()]
    orders = [dict(r) for r in db.execute(
        'SELECT * FROM payment_orders WHERE org_id=? ORDER BY created_at DESC LIMIT 20', (org_id,)).fetchall()]
    # Produtos para o select
    products = [dict(r) for r in db.execute(
        'SELECT id, title, price FROM mp_products WHERE org_id=? AND status=? ORDER BY title', (org_id, 'active')).fetchall()]
    db.close()

    total_links = len(links)
    paid_links = [l for l in links if l['status'] == 'paid']
    pending_links = [l for l in links if l['status'] == 'pending']
    total_revenue = sum(l['price'] for l in paid_links)
    total_views = sum(l['views'] for l in links)

    return render_template('pagamentos.html', page='pagamentos',
                           links=links, orders=orders, products=products,
                           total_links=total_links, paid_count=len(paid_links),
                           pending_count=len(pending_links), total_revenue=total_revenue,
                           total_views=total_views)


@app.route('/api/pagamento/create', methods=['POST'])
@login_required
def api_create_payment():
    org_id = session.get('org_id', 1)
    data = request.get_json(silent=True) or {}
    product_id = data.get('product_id')
    product_title = data.get('product_title', '').strip()
    price = float(data.get('price', 0))
    description = data.get('description', '').strip()

    if not product_title or price <= 0:
        return jsonify({'ok': False, 'error': 'Titulo e preco obrigatorios'}), 400

    code = _gen_pay_code()
    db = get_db()
    # Garantir codigo unico
    while db.execute('SELECT id FROM payment_links WHERE code=?', (code,)).fetchone():
        code = _gen_pay_code()
    db.execute('''INSERT INTO payment_links (org_id,code,product_id,product_title,price,description)
                  VALUES (?,?,?,?,?,?)''',
               (org_id, code, product_id, product_title, price, description))
    db.commit()
    db.close()
    return jsonify({'ok': True, 'code': code, 'url': f'/pay/{code}'})


@app.route('/pay/<code>')
def payment_page(code):
    """Pagina publica de pagamento — sem login."""
    db = get_db()
    link = db.execute('SELECT pl.*, mlc.store_name, mlc.whatsapp, mlc.accent_color, mlc.logo_url '
                      'FROM payment_links pl '
                      'LEFT JOIN mini_loja_config mlc ON pl.org_id = mlc.org_id '
                      'WHERE pl.code=? AND pl.is_active=1', (code,)).fetchone()
    if not link:
        db.close()
        return render_template('mini_loja_404.html'), 404
    link = dict(link)
    # Incrementar views
    db.execute('UPDATE payment_links SET views=views+1 WHERE code=?', (code,))
    db.commit()
    db.close()
    return render_template('payment_public.html', link=link)


@app.route('/api/pay/<code>/order', methods=['POST'])
def api_submit_order(code):
    """Submeter pedido via link de pagamento."""
    db = get_db()
    link = db.execute('SELECT * FROM payment_links WHERE code=? AND is_active=1', (code,)).fetchone()
    if not link:
        db.close()
        return jsonify({'ok': False, 'error': 'Link invalido'}), 404

    data = request.get_json(silent=True) or {}
    name = data.get('name', '').strip()
    phone = data.get('phone', '').strip()
    email = data.get('email', '').strip()
    address = data.get('address', '').strip()
    city = data.get('city', '').strip()
    state = data.get('state', '').strip()
    cep = data.get('cep', '').strip()
    notes = data.get('notes', '').strip()

    if not name or not phone:
        db.close()
        return jsonify({'ok': False, 'error': 'Nome e telefone obrigatorios'}), 400

    db.execute('''INSERT INTO payment_orders
        (org_id,payment_link_id,customer_name,customer_phone,customer_email,address,city,state,cep,notes,total)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
        (link['org_id'], link['id'], name, phone, email, address, city, state, cep, notes, link['price']))

    # Atualizar link com dados do cliente
    db.execute('UPDATE payment_links SET customer_name=?, customer_phone=?, customer_email=?, status=? WHERE id=?',
               (name, phone, email, 'pending', link['id']))
    db.commit()
    db.close()

    # Montar link WhatsApp de notificacao
    wa_number = ''
    try:
        db2 = get_db()
        mlc = db2.execute('SELECT whatsapp FROM mini_loja_config WHERE org_id=?', (link['org_id'],)).fetchone()
        if mlc and mlc['whatsapp']:
            wa_number = mlc['whatsapp']
        db2.close()
    except Exception:
        pass

    return jsonify({'ok': True, 'whatsapp': wa_number, 'product': link['product_title']})


# ── Vulnerability Score ──────────────────────────────────────────────────────

@app.route('/vulnerability')
@login_required
@plan_required('vulnerability')
def vulnerability():
    from vulnerability_engine import compute_store_vulnerability, get_alerts
    org_id = session.get('org_id', 1)
    data = compute_store_vulnerability(org_id)
    alerts_data = get_alerts(org_id, limit=20)
    return render_template('vulnerability.html', page='vulnerability', data=data, alerts=alerts_data)


@app.route('/api/vulnerability/recalculate', methods=['POST'])
@login_required
def api_vulnerability_recalculate():
    from vulnerability_engine import compute_store_vulnerability
    import traceback
    org_id = session.get('org_id', 1)
    try:
        data = compute_store_vulnerability(org_id)
        return jsonify({'ok': True, 'store_score': data['store_score'], 'total': data['total_products']})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e), 'trace': traceback.format_exc()[:500]})


@app.route('/api/vulnerability/alerts')
@login_required
def api_vulnerability_alerts():
    from vulnerability_engine import get_alerts
    org_id = session.get('org_id', 1)
    unread = request.args.get('unread', '0') == '1'
    data = get_alerts(org_id, unread_only=unread)
    return jsonify(data)


@app.route('/api/vulnerability/dismiss-alert/<int:alert_id>', methods=['POST'])
@login_required
def api_dismiss_alert(alert_id):
    from vulnerability_engine import dismiss_alert
    org_id = session.get('org_id', 1)
    dismiss_alert(alert_id, org_id)
    return jsonify({'ok': True})


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
        # Handle plan change
        new_plan = request.form.get('plan', '')
        if new_plan in PLAN_ACCESS:
            db.execute('UPDATE organizations SET plan=? WHERE id=?', (new_plan, session['org_id']))
            session['plan'] = new_plan
        msg = 'Alterações salvas com sucesso!'
    return render_template('integrations.html', msg=msg)


@app.route('/api/change-plan', methods=['POST'])
@login_required
def api_change_plan():
    """Change org plan via API."""
    plan = request.json.get('plan', '') if request.is_json else request.form.get('plan', '')
    if plan not in PLAN_ACCESS:
        return jsonify({'error': 'Plano invalido'}), 400
    db = get_db()
    db.execute('UPDATE organizations SET plan=? WHERE id=?', (plan, session['org_id']))
    db.commit()
    db.close()
    session['plan'] = plan
    return jsonify({'ok': True, 'plan': plan, 'label': PLAN_ACCESS[plan]['label']})


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
@plan_required('traffic')
def traffic():
    import re as _re
    db = get_db()
    org_id = session.get('org_id', 1)

    # ── Auto-sync Meta Ads if connected ─────────────────────
    try:
        meta_row = db.execute(
            "SELECT status FROM api_integrations WHERE org_id=? AND platform='meta_ads'",
            (org_id,)
        ).fetchone()
        if meta_row and meta_row['status'] == 'connected':
            from sync_meta_ads import sync_all as meta_sync
            from sync_base import run_sync_if_needed
            run_sync_if_needed(org_id, 'meta_ads', meta_sync, max_age=60)
    except Exception:
        pass

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
    all_camps = db.execute(sql, params).fetchall()

    # ── Filter demo data when real data exists ──────────────
    _DEMO_RE = _re.compile(r'^(meta|goog|tik)_\d+$')
    real_camps = [c for c in all_camps if not _DEMO_RE.match(dict(c).get('external_campaign_id') or '')]
    campaigns_raw = real_camps if real_camps else all_camps
    
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
    
    # Split by platform for separate tables
    meta_campaigns = [c for c in campaigns if c.get('platform') == 'meta']
    google_campaigns = [c for c in campaigns if c.get('platform') == 'google']

    return render_template('trafego_pago.html',
                         campaigns=campaigns, kpis=kpis,
                         platforms=platforms, insights=ai_insights, ai_global_roas=ai_global_roas,
                         meta_campaigns=meta_campaigns, google_campaigns=google_campaigns)

# -- AI Apply endpoint for Traffic campaigns --

def _meta_api_call(token, campaign_id, payload):
    """Faz chamada POST na Meta Graph API para alterar campanha."""
    import urllib.request, urllib.parse, json as _json
    url = f"https://graph.facebook.com/v18.0/{campaign_id}?access_token={urllib.parse.quote(token)}"
    body = _json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=body, headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, _json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, _json.loads(e.read().decode('utf-8', errors='replace'))


def _get_meta_token(db, org_id):
    """Busca o token Meta Ads salvo para a org."""
    row = db.execute(
        "SELECT access_token FROM api_integrations WHERE org_id=? AND platform='meta_ads' AND status='connected'",
        (org_id,)
    ).fetchone()
    return row['access_token'] if row else None


@app.route('/traffic/ai-apply', methods=['POST'])
@login_required
def traffic_ai_apply():
    """Aplica sugestao da IA em uma campanha — executa acoes reais na plataforma."""
    import datetime
    try:
        data = request.get_json() or {}
        platform       = data.get('platform', '')
        campaign_id    = data.get('campaign_id', '')
        campaign_name  = data.get('campaign_name', '')
        action         = data.get('action', '')
        suggestion_idx = data.get('suggestion_index', 0)
        suggestion_txt = data.get('suggestion_text', '')
        apply_all      = data.get('apply_all', False)

        db = get_db()
        org_id = session.get('org_id', 1)

        # ── Ensure log table exists ──────────────────────────────
        db.execute("""
            CREATE TABLE IF NOT EXISTS ai_actions_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER,
                platform TEXT,
                campaign_id TEXT,
                campaign_name TEXT,
                action_type TEXT,
                suggestion TEXT,
                applied_at TEXT,
                status TEXT DEFAULT 'applied',
                api_result TEXT
            )
        """)

        api_actions_done = []
        api_errors = []

        # ── Execute real actions on Meta Ads API ─────────────────
        if platform == 'meta' and campaign_id:
            token = _get_meta_token(db, org_id)
            if token:
                from sync_base import api_request as _api_req
                import urllib.parse as _up

                if action == 'pause':
                    # Pausar campanha na Meta
                    status_code, result = _meta_api_call(token, campaign_id, {'status': 'PAUSED'})
                    if status_code == 200 and result.get('success'):
                        api_actions_done.append('Campanha pausada no Meta Ads')
                        db.execute("UPDATE ad_campaigns SET status='paused' WHERE external_campaign_id=? AND org_id=?",
                                   (campaign_id, org_id))
                    else:
                        api_errors.append(f"Erro ao pausar: {result.get('error', {}).get('message', 'Erro desconhecido')}")

                elif action == 'scale':
                    # Buscar adsets da campanha e aumentar budget em 25%
                    try:
                        adsets_data = _api_req(
                            f"https://graph.facebook.com/v18.0/{campaign_id}/adsets?access_token={_up.quote(token)}&fields=id,name,daily_budget,lifetime_budget,status"
                        )
                        adsets = adsets_data.get('data', [])
                        for adset in adsets:
                            if adset.get('daily_budget'):
                                old_budget = int(adset['daily_budget'])
                                new_budget = int(old_budget * 1.25)
                                sc, res = _meta_api_call(token, adset['id'], {'daily_budget': str(new_budget)})
                                if sc == 200 and res.get('success'):
                                    api_actions_done.append(f"Budget diario de {adset.get('name','')} aumentado de R$ {old_budget/100:.2f} para R$ {new_budget/100:.2f}")
                                else:
                                    api_errors.append(f"Erro no adset {adset.get('name','')}: {res.get('error',{}).get('message','')}")
                            elif adset.get('lifetime_budget'):
                                old_budget = int(adset['lifetime_budget'])
                                new_budget = int(old_budget * 1.25)
                                sc, res = _meta_api_call(token, adset['id'], {'lifetime_budget': str(new_budget)})
                                if sc == 200 and res.get('success'):
                                    api_actions_done.append(f"Budget vitalicio de {adset.get('name','')} aumentado em 25%")
                                else:
                                    api_errors.append(f"Erro no adset {adset.get('name','')}: {res.get('error',{}).get('message','')}")
                        if not adsets:
                            api_errors.append('Nenhum conjunto de anuncios encontrado nesta campanha')
                    except Exception as e:
                        api_errors.append(f"Erro ao buscar adsets: {str(e)}")

                elif action == 'optimize':
                    # Para otimizar: pausar adsets com baixo desempenho
                    try:
                        _adsets_url = (f"https://graph.facebook.com/v18.0/{campaign_id}/adsets"
                                       f"?access_token={_up.quote(token)}"
                                       f"&fields=id,name,status")
                        adsets = _api_req(_adsets_url).get('data', [])
                        # Buscar insights de cada adset para identificar os piores
                        adsets_data = []
                        for adset in adsets:
                            _ins_url = (f"https://graph.facebook.com/v18.0/{adset['id']}/insights"
                                        f"?access_token={_up.quote(token)}"
                                        f"&fields=spend,actions&date_preset=last_7d")
                            ins_data = _api_req(_ins_url).get('data', [{}])
                            spend = float(ins_data[0].get('spend', 0)) if ins_data else 0
                            conversions = 0
                            for a in (ins_data[0].get('actions', []) if ins_data else []):
                                if a.get('action_type') in ('purchase', 'offsite_conversion.fb_pixel_purchase', 'lead'):
                                    conversions += int(a.get('value', 0))
                            adsets_data.append({**adset, 'spend': spend, 'conversions': conversions})

                        # Pausar adsets com gasto > 0 mas 0 conversoes
                        for ad in adsets_data:
                            if ad['spend'] > 0 and ad['conversions'] == 0 and ad.get('status') == 'ACTIVE':
                                sc, res = _meta_api_call(token, ad['id'], {'status': 'PAUSED'})
                                if sc == 200 and res.get('success'):
                                    api_actions_done.append(f"Adset '{ad.get('name','')}' pausado (R$ {ad['spend']:.2f} gasto sem conversoes)")
                                else:
                                    api_errors.append(f"Erro ao pausar adset {ad.get('name','')}")

                        if not api_actions_done and not api_errors:
                            api_actions_done.append('Todos os conjuntos de anuncios tem desempenho aceitavel — nenhuma mudanca necessaria agora')

                    except Exception as e:
                        api_errors.append(f"Erro ao otimizar: {str(e)}")

        # ── Log the action ───────────────────────────────────────
        log_status = 'executed' if api_actions_done else ('error' if api_errors else 'registered')
        api_result_text = '; '.join(api_actions_done + api_errors)
        try:
            db.execute("""
                INSERT INTO ai_actions_log (org_id, platform, campaign_id, campaign_name, action_type, suggestion, applied_at, status, api_result)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (org_id, platform, campaign_id, campaign_name, action,
                  suggestion_txt if isinstance(suggestion_txt, str) else str(suggestion_txt),
                  datetime.datetime.now().isoformat(), log_status, api_result_text))
            db.commit()
        except Exception:
            pass

        # ── Build response ───────────────────────────────────────
        if api_actions_done:
            title = 'Alteracoes aplicadas no Meta Ads' if not apply_all else 'Todas as sugestoes aplicadas no Meta Ads'
            actions_html = '<br>'.join(['✅ ' + a for a in api_actions_done])
            if api_errors:
                actions_html += '<br>' + '<br>'.join(['⚠️ ' + e for e in api_errors])
            message = actions_html
        elif api_errors:
            title = 'Erro ao aplicar no Meta Ads'
            message = '<br>'.join(['❌ ' + e for e in api_errors])
        else:
            # No API token or Google Ads (no API yet) — register only
            action_labels = {'scale': 'Escalar', 'optimize': 'Otimizar', 'pause': 'Pausar'}
            title = action_labels.get(action, 'Acao IA') + ': ' + campaign_name
            if platform == 'google':
                message = 'Sugestao registrada. A integracao com Google Ads API sera adicionada em breve. Por enquanto, aplique manualmente no Google Ads.'
            else:
                message = 'Sugestao registrada. Conecte sua conta Meta Ads em Integracoes para que as acoes sejam aplicadas automaticamente.'

        return jsonify({
            'status': 'ok',
            'title': title,
            'message': message,
            'actions_done': api_actions_done,
            'errors': api_errors
        })

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


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

@app.route('/api/debug/orders')
def debug_orders():
    """Show all ML orders with status and revenue for debugging."""
    org_id = 1
    mp = request.args.get('mp', 'mercado_livre')
    db = get_db()
    rows = db.execute(
        "SELECT external_id, status, gmv, revenue, ordered_at FROM orders WHERE org_id=? AND marketplace=? ORDER BY ordered_at DESC",
        (org_id, mp)
    ).fetchall()
    orders_list = [dict(r) for r in rows]
    total_revenue = sum(o['revenue'] for o in orders_list)
    total_gmv = sum(o['gmv'] for o in orders_list)
    paid_orders = [o for o in orders_list if o['revenue'] > 0]
    db.close()
    return jsonify({
        'total_orders': len(orders_list),
        'paid_orders': len(paid_orders),
        'total_revenue': total_revenue,
        'total_gmv': total_gmv,
        'orders': orders_list
    })


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


# ══════════════════════════════════════════════════════════════════════════
#  ADMIN / AUTO-IMPROVEMENT ENDPOINTS
#  Rotas internas para controlar feature flags, ver insights e disparar IA.
# ══════════════════════════════════════════════════════════════════════════

@app.route('/admin/insights')
@login_required
def admin_insights():
    """Lista insights gerados pela IA de auto-melhoria."""
    import auto_insights
    status = request.args.get('status', 'new')
    insights = auto_insights.get_recent_insights(status=status, limit=100)
    return jsonify({'ok': True, 'insights': insights, 'count': len(insights)})


@app.route('/admin/insights/run', methods=['POST'])
@login_required
def admin_insights_run():
    """Dispara analise manual (normalmente roda via cron a cada 6h)."""
    import auto_insights
    auto_insights.run_all()
    return jsonify({'ok': True, 'msg': 'analise disparada'})


@app.route('/admin/insights/<int:insight_id>/review', methods=['POST'])
@login_required
def admin_insight_review(insight_id):
    """Marca um insight como revisado."""
    import auto_insights
    auto_insights.mark_reviewed(insight_id, 'reviewed')
    return jsonify({'ok': True})


@app.route('/admin/flags')
@login_required
def admin_flags():
    """Lista todas as feature flags."""
    import feature_flags
    return jsonify({'ok': True, 'flags': feature_flags.all_flags()})


@app.route('/admin/flags/<flag_name>', methods=['POST'])
@login_required
def admin_flag_update(flag_name):
    """Atualiza uma feature flag (enabled, rollout_pct, whitelist)."""
    import feature_flags
    data = request.get_json() or {}
    try:
        feature_flags.set_flag(
            flag_name,
            enabled=data.get('enabled'),
            rollout_pct=data.get('rollout_pct'),
            whitelist=data.get('whitelist'),
        )
        return jsonify({'ok': True, 'flag': flag_name})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 400


@app.route('/admin/flags/<flag_name>/rollback', methods=['POST'])
@login_required
def admin_flag_rollback(flag_name):
    """Rollback instantaneo de uma feature flag."""
    import feature_flags
    feature_flags.rollback(flag_name)
    return jsonify({'ok': True, 'msg': f'{flag_name} rolled back'})


@app.route('/admin/pricing/suggest', methods=['POST'])
@login_required
def admin_pricing_suggest():
    """Retorna sugestao de preco para um SKU especifico."""
    import pricing_ai
    data = request.get_json() or {}
    org_id = session.get('org_id', 1)
    sku = data.get('sku')
    marketplace = data.get('marketplace', 'mercado_livre')
    if not sku:
        return jsonify({'ok': False, 'error': 'sku required'}), 400
    suggestion = pricing_ai.suggest_price(org_id, sku, marketplace)
    return jsonify({'ok': True, 'suggestion': suggestion})


@app.route('/admin/pricing/run-batch', methods=['POST'])
@login_required
def admin_pricing_run_batch():
    """Roda pricing AI em todos os SKUs com auto_apply=1."""
    import pricing_ai
    org_id = session.get('org_id', 1)
    result = pricing_ai.run_pricing_batch(org_id)
    return jsonify({'ok': True, 'result': result})


@app.route('/admin/pricing/stats')
@login_required
def admin_pricing_stats():
    """Estatisticas do pricing AI."""
    import pricing_ai
    org_id = session.get('org_id', 1)
    return jsonify({'ok': True, 'stats': pricing_ai.get_pricing_stats(org_id)})


@app.route('/admin/telemetry/events')
@login_required
def admin_telemetry_events():
    """Ultimos eventos de telemetria."""
    import telemetry
    org_id = session.get('org_id', 1)
    event_type = request.args.get('type')
    limit = int(request.args.get('limit', 100))
    events = telemetry.get_recent_events(org_id=org_id, limit=limit, event_type=event_type)
    return jsonify({'ok': True, 'events': events, 'count': len(events)})


@app.route('/admin')
@login_required
def admin_panel():
    """Dashboard admin visual para auto-improvement."""
    return render_template('admin_panel.html')


@app.route('/admin/wa/stats')
@login_required
def admin_wa_stats():
    """Estatisticas do agente WhatsApp."""
    import whatsapp_agent
    org_id = session.get('org_id', 1)
    days = int(request.args.get('days', 7))
    return jsonify({'ok': True, 'stats': whatsapp_agent.get_agent_stats(org_id, days)})


@app.route('/admin/wa/conversations')
@login_required
def admin_wa_conversations():
    """Lista conversas recentes."""
    import whatsapp_agent
    org_id = session.get('org_id', 1)
    return jsonify({'ok': True, 'conversations': whatsapp_agent.get_conversations(org_id)})


@app.route('/admin/buybox/stats')
@login_required
def admin_buybox_stats():
    """Estatisticas de monitoramento de Buy Box."""
    import buybox_monitor
    org_id = session.get('org_id', 1)
    return jsonify({'ok': True, 'stats': buybox_monitor.get_stats(org_id)})


@app.route('/admin/buybox/alerts')
@login_required
def admin_buybox_alerts():
    """Alertas de Buy Box pendentes."""
    import buybox_monitor
    org_id = session.get('org_id', 1)
    return jsonify({'ok': True, 'alerts': buybox_monitor.get_alerts(org_id)})


@app.route('/admin/buybox/status')
@login_required
def admin_buybox_status():
    """Status atual de todos os SKUs monitorados."""
    import buybox_monitor
    org_id = session.get('org_id', 1)
    marketplace = request.args.get('mp')
    return jsonify({'ok': True, 'status': buybox_monitor.get_current_status(org_id, marketplace)})


@app.route('/admin/fraud/score', methods=['POST'])
@login_required
def admin_fraud_score():
    """Analisa uma devolucao e retorna score + decisao."""
    import fraud_detector
    org_id = session.get('org_id', 1)
    data = request.get_json() or {}
    result = fraud_detector.score_return(org_id, data)
    return jsonify({'ok': True, 'result': result})


@app.route('/admin/fraud/pending')
@login_required
def admin_fraud_pending():
    """Lista devolucoes aguardando revisao manual."""
    import fraud_detector
    org_id = session.get('org_id', 1)
    return jsonify({'ok': True, 'pending': fraud_detector.get_pending_reviews(org_id)})


@app.route('/admin/fraud/stats')
@login_required
def admin_fraud_stats():
    """Estatisticas de fraud detection."""
    import fraud_detector
    org_id = session.get('org_id', 1)
    return jsonify({'ok': True, 'stats': fraud_detector.get_stats(org_id)})


@app.route('/admin/fraud/resolve/<int:score_id>', methods=['POST'])
@login_required
def admin_fraud_resolve(score_id):
    """Resolve uma revisao (approve ou block)."""
    import fraud_detector
    data = request.get_json() or {}
    decision = data.get('decision', 'approve')
    if decision not in ('approve', 'block'):
        return jsonify({'ok': False, 'error': 'invalid decision'}), 400
    fraud_detector.resolve_review(score_id, decision, reviewer='admin')
    return jsonify({'ok': True})


@app.route('/admin/content/generate', methods=['POST'])
@login_required
def admin_content_generate():
    """Gera titulo/descricao/bullets/tags otimizados."""
    import content_ai
    org_id = session.get('org_id', 1)
    data = request.get_json() or {}
    marketplace = data.get('marketplace', 'mercado_livre')
    result = content_ai.generate_full(org_id, data, marketplace, save=True)
    return jsonify({'ok': True, 'content': result})


@app.route('/admin/content/recent')
@login_required
def admin_content_recent():
    """Lista ultimas geracoes de conteudo."""
    import content_ai
    org_id = session.get('org_id', 1)
    return jsonify({'ok': True, 'generations': content_ai.get_recent_generations(org_id)})


@app.route('/admin/cohorts')
@login_required
def admin_cohorts():
    """Relatorio completo de cohort analytics."""
    import cohort_analytics
    org_id = session.get('org_id', 1)
    return jsonify({'ok': True, 'report': cohort_analytics.compute_full_report(org_id)})


@app.route('/admin/cohorts/monthly')
@login_required
def admin_cohorts_monthly():
    """Apenas retencao mensal."""
    import cohort_analytics
    org_id = session.get('org_id', 1)
    return jsonify({'ok': True, 'cohorts': cohort_analytics.get_monthly_cohorts(org_id)})


@app.route('/api/wa/incoming', methods=['POST'])
def api_wa_incoming():
    """Webhook do WhatsApp Business API (publico)."""
    import whatsapp_agent
    data = request.get_json() or {}
    org_id = data.get('org_id', 1)
    phone = data.get('phone', '')
    name = data.get('name', 'cliente')
    text = data.get('text', '')
    if not phone or not text:
        return jsonify({'ok': False, 'error': 'phone and text required'}), 400
    response = whatsapp_agent.handle_incoming_message(org_id, phone, name, text)
    if response is None:
        return jsonify({'ok': True, 'handoff': True, 'msg': 'transferido para humano'})
    return jsonify({'ok': True, 'response': response})


if __name__ == '__main__':
    app.run(debug=True)
