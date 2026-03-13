from flask import Flask, render_template, request, session, redirect, url_for, jsonify, send_file
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
                                   analyze_mp_ads, get_keyword_opportunities)
    mp  = request.args.get('mp', 'mercado_livre')
    tab = request.args.get('tab', 'overview')
    date_start = request.args.get('date_start', '')
    date_end = request.args.get('date_end', '')
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

    # Aggregate marketplace totals from orders (with date filter)
    mp_totals = {}
    for m_id in ['mercado_livre', 'amazon', 'tiktok_shop']:
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

    return render_template('traffic.html', mp=mp, tab=tab, all_mp=all_mp, health=health,
                           competitors=competitors, my=my_product, comp_analysis=analysis,
                           ads=ads, returns=returns, keywords=keywords, stock_items=stock_items,
                           mp_totals=mp_totals, date_start=date_start, date_end=date_end)

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
            elif platform == 'google_analytics':
                env_vars = ['GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET']
            elif platform == 'bling':
                env_vars = ['BLING_CLIENT_ID', 'BLING_CLIENT_SECRET']
            return render_template('oauth_not_configured.html',
                                   platform_name=cat.get('name', platform),
                                   env_vars=env_vars)
        org_id = session.get('org_id', 1)
        url    = build_auth_url(platform, org_id, request.host)
        return redirect(url)
    # Re-fetch all platforms so the grid doesn't disappear
    from oauth_manager import get_all_integrations, is_app_configured
    org_id = session.get('org_id', 1)
    connected_map = get_all_integrations(org_id)
    all_platforms = []
    for k, info in INTEGRATIONS_CATALOG.items():
        conn = connected_map.get(k, {})
        all_platforms.append({**info, 'key': k,
                              'connected': conn.get('status') == 'connected',
                              'configured': is_app_configured(k),
                              'account_name': conn.get('account_name', ''),
                              'last_sync': conn.get('last_sync', '')})
    return render_template('integrations_hub.html', platforms=all_platforms, api_key_platform=platform, catalog_item=cat)

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


# ══ ROTAS DE RELATORIOS EXPORTAVEIS ═══════════════════════════════
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
