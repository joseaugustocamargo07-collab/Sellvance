from flask import Flask, render_template, request, session, redirect, url_for, jsonify
from database import get_db
from auth import login_required, verify_password, hash_password
from traffic_ai import analyze_all, calc_metrics, score_campaign
import os

app = Flask(__name__, template_folder='.')
app.secret_key = os.environ.get('SECRET_KEY', 'sellvance-secret-2026-change-in-prod')

_db_ready = False

@app.before_request
def ensure_db_ready():
    global _db_ready
    if not _db_ready:
        _db_ready = True
        try:
            from database import init_db, migrate_db
            init_db()
            migrate_db()
        except Exception as e:
            import traceback
            traceback.print_exc()

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'app': 'Sellvance CRM'}), 200

@app.route('/')
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
    from marketplace_intel import (COMPETITORS, MY_PRODUCTS, MP_ADS_DATA, RETURNS_DATA,
                                   ACCOUNT_HEALTH, analyze_competitive_position,
                                   analyze_mp_ads, get_keyword_opportunities)
    mp  = request.args.get('mp', 'mercado_livre')
    tab = request.args.get('tab', 'overview')
    all_mp = [
        {'id': 'mercado_livre', 'name': 'Mercado Livre', 'icon': '🛒', 'color': '#ffe600'},
        {'id': 'amazon',        'name': 'Amazon',        'icon': '📦', 'color': '#ff9900'},
        {'id': 'tiktok_shop',   'name': 'TikTok Shop',   'icon': '🎵', 'color': '#ff0050'},
    ]
    health      = ACCOUNT_HEALTH.get(mp, {'score': 0, 'metrics': {}, 'alerts': []})
    competitors = COMPETITORS.get(mp, [])
    my_product  = MY_PRODUCTS.get(mp, {})
    analysis    = analyze_competitive_position(mp)
    ads         = analyze_mp_ads(mp)
    returns     = RETURNS_DATA.get(mp, {})
    keywords    = get_keyword_opportunities(mp)

    db          = get_db()
    org_id      = session.get('org_id', 1)
    stock_items = db.execute('SELECT * FROM stock_items WHERE org_id = ? AND marketplace = ?',
                             (org_id, mp)).fetchall()

    return render_template('traffic.html', mp=mp, tab=tab, all_mp=all_mp, health=health,
                           competitors=competitors, my=my_product, comp_analysis=analysis,
                           ads=ads, returns=returns, keywords=keywords, stock_items=stock_items)

@app.route('/integrations')
@login_required
def integrations():
    from integrations import INTEGRATIONS_CATALOG
    from oauth_manager import get_all_integrations, is_app_configured
    org_id    = session.get('org_id', 1)
    connected = get_all_integrations(org_id)
    connected_map = {i['platform']: dict(i) for i in connected}
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
    from oauth_manager import build_auth_url, is_app_configured, OAUTH_APPS
    from integrations import INTEGRATIONS_CATALOG
    cat = INTEGRATIONS_CATALOG.get(platform, {})
    if cat.get('auth_type') == 'oauth2':
        if not is_app_configured(platform):
            app_info = OAUTH_APPS.get(platform, {})
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
            return render_template('oauth_not_configured.html',
                                   platform_name=cat.get('name', platform),
                                   env_vars=env_vars)
        org_id = session.get('org_id', 1)
        url    = build_auth_url(platform, org_id, request.host)
        return redirect(url)
    return render_template('integrations_hub.html', platforms=[], api_key_platform=platform, catalog_item=cat)

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
        token_data   = exchange_code_for_token(platform, code, request.host)
        access_token = token_data.get('access_token', '')
        account_info = {}
        if platform == 'mercado_livre' and access_token:
            account_info = fetch_ml_account_info(access_token)
        elif platform == 'meta_ads' and access_token:
            account_info = fetch_meta_account_info(access_token)
        elif platform == 'google_ads' and access_token:
            account_info = fetch_google_account_info(access_token)
        save_integration(org_id, platform, token_data, account_info)
        account_name = account_info.get('nickname') or account_info.get('name') or platform
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

if __name__ == '__main__':
    app.run(debug=True)
