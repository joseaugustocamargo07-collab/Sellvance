from flask import Flask, render_template, request, session, redirect, url_for, jsonify
from database import get_db
from auth import login_required, verify_password, hash_password
from traffic_ai import analyze_all, calc_metrics, score_campaign
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'sellvance-secret-2026-change-in-prod')

# Inicializa banco lazy (na primeira requisição) para não atrasar o startup
_db_ready = False

@app.before_request
def ensure_db_ready():
    global _db_ready
    if not _db_ready:
        _db_ready = True
        from database import init_db, migrate_db
        init_db()
        migrate_db()

# ===== ROTA CRÍTICA PARA RAILWAY HEALTHCHECK =====
@app.route('/health')
def health():
    """Healthcheck para Railway"""
    try:
        db = get_db()
        # Testa conexão simples
        result = db.execute("SELECT 1").fetchone()
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': __import__('datetime').datetime.now().isoformat(),
            'app': 'Sellvance CRM'
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': __import__('datetime').datetime.now().isoformat()
        }), 500

# ===== ROTAS PRINCIPAIS =====
@app.route('/')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login'))

    db = get_db()

    # Métricas do dashboard
    campaigns = db.execute('SELECT COUNT(*) as count FROM campaigns').fetchone()
    customers = db.execute('SELECT COUNT(*) as count FROM customers').fetchone()
    revenue = db.execute('SELECT COALESCE(SUM(revenue), 0) as total FROM campaigns').fetchone()

    # Campanhas recentes
    recent_campaigns = db.execute('''
        SELECT id, name, platform, budget, revenue,
               ROUND((revenue * 100.0 / budget), 1) as roi
        FROM campaigns
        ORDER BY created_at DESC
        LIMIT 5
    ''').fetchall()

    return render_template('dashboard.html',
                         campaigns=campaigns['count'],
                         customers=customers['count'],
                         revenue=revenue['total'],
                         recent_campaigns=recent_campaigns)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']

        db = get_db()
        user = db.execute('SELECT * FROM users WHERE email = ?', (email,)).fetchone()

        if user and verify_password(user['password_hash'], password):
            session['user_id'] = user['id']
            session['user_name'] = user['name']
            return redirect(url_for('dashboard'))
        else:
            return render_template('login.html', error='Email ou senha inválidos')

    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

# ===== CRM COM ANÁLISE RFM =====
@app.route('/crm')
@login_required
def crm():
    db = get_db()

    # Busca com filtros opcionais
    search = request.args.get('search', '')
    segment = request.args.get('segment', '')

    query = '''
        SELECT c.*,
               CASE
                   WHEN r_score = 5 AND f_score = 5 AND m_score >= 4 THEN 'Champions'
                   WHEN r_score = 5 AND f_score >= 2 AND f_score <= 4 AND m_score >= 3 THEN 'Loyal Customers'
                   WHEN r_score >= 4 AND f_score < 2 THEN 'Potential Loyalists'
                   WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'New Customers'
                   WHEN r_score >= 2 AND r_score <= 3 AND f_score <= 2 THEN 'Promising'
                   WHEN r_score >= 2 AND r_score <= 3 AND f_score >= 3 AND m_score <= 2 THEN 'Need Attention'
                   WHEN r_score >= 2 AND r_score <= 3 AND f_score >= 3 AND m_score >= 3 THEN 'About to Sleep'
                   WHEN r_score <= 2 AND f_score >= 4 THEN 'At Risk'
                   WHEN r_score = 1 AND f_score = 1 THEN 'Lost'
                   ELSE 'Cannot Lose Them'
               END as segment
        FROM customers c
        WHERE 1=1
    '''

    params = []
    if search:
        query += ' AND (name LIKE ? OR email LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%'])

    if segment:
        # Adiciona filtro por segmento (seria necessário subconsulta mais complexa)
        pass

    query += ' ORDER BY total_value DESC LIMIT 100'

    customers = db.execute(query, params).fetchall()

    # Estatísticas por segmento
    segment_stats = db.execute('''
        SELECT
            CASE
                WHEN r_score = 5 AND f_score = 5 AND m_score >= 4 THEN 'Champions'
                WHEN r_score = 5 AND f_score >= 2 AND f_score <= 4 AND m_score >= 3 THEN 'Loyal Customers'
                WHEN r_score >= 4 AND f_score < 2 THEN 'Potential Loyalists'
                WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'New Customers'
                WHEN r_score >= 2 AND r_score <= 3 AND f_score <= 2 THEN 'Promising'
                WHEN r_score >= 2 AND r_score <= 3 AND f_score >= 3 AND m_score <= 2 THEN 'Need Attention'
                WHEN r_score >= 2 AND r_score <= 3 AND f_score >= 3 AND m_score >= 3 THEN 'About to Sleep'
                WHEN r_score <= 2 AND f_score >= 4 THEN 'At Risk'
                WHEN r_score = 1 AND f_score = 1 THEN 'Lost'
                ELSE 'Cannot Lose Them'
            END as segment,
            COUNT(*) as count,
            SUM(total_value) as total_revenue
        FROM customers
        GROUP BY segment
        ORDER BY total_revenue DESC
    ''').fetchall()

    return render_template('crm.html',
                         customers=customers,
                         segment_stats=segment_stats,
                         search=search,
                         current_segment=segment)

@app.route('/customer/<int:customer_id>')
@login_required
def customer_detail(customer_id):
    db = get_db()

    customer = db.execute('SELECT * FROM customers WHERE id = ?', (customer_id,)).fetchone()
    if not customer:
        return "Cliente não encontrado", 404

    # Histórico de compras (simulado)
    purchases = db.execute('''
        SELECT * FROM customer_purchases
        WHERE customer_id = ?
        ORDER BY purchase_date DESC
    ''', (customer_id,)).fetchall()

    # Campanhas que impactaram este cliente
    campaigns = db.execute('''
        SELECT DISTINCT c.* FROM campaigns c
        JOIN campaign_customers cc ON c.id = cc.campaign_id
        WHERE cc.customer_id = ?
        ORDER BY c.created_at DESC
    ''', (customer_id,)).fetchall()

    return render_template('customer_detail.html',
                         customer=customer,
                         purchases=purchases,
                         campaigns=campaigns)

# ===== TRÁFEGO PAGO COM IA =====
@app.route('/traffic')
@login_required
def traffic():
    db = get_db()

    # Lista campanhas com métricas
    campaigns = db.execute('''
        SELECT *,
               ROUND((revenue * 100.0 / budget), 1) as roi,
               ROUND((revenue - budget), 2) as profit,
               ROUND((clicks * 100.0 / impressions), 2) as ctr,
               ROUND((conversions * 100.0 / clicks), 2) as conversion_rate
        FROM campaigns
        ORDER BY created_at DESC
    ''').fetchall()

    # Análise com IA
    analysis = analyze_all(campaigns)

    return render_template('traffic.html',
                         campaigns=campaigns,
                         analysis=analysis)

@app.route('/traffic/campaign/<int:campaign_id>')
@login_required
def campaign_detail(campaign_id):
    db = get_db()

    campaign = db.execute('SELECT * FROM campaigns WHERE id = ?', (campaign_id,)).fetchone()
    if not campaign:
        return "Campanha não encontrada", 404

    # Métricas calculadas
    metrics = calc_metrics(campaign)

    # Score da campanha
    score = score_campaign(campaign)

    # Dados diários (simulado)
    daily_data = db.execute('''
        SELECT date, impressions, clicks, conversions, cost, revenue
        FROM campaign_daily_data
        WHERE campaign_id = ?
        ORDER BY date
    ''', (campaign_id,)).fetchall()

    return render_template('campaign_detail.html',
                         campaign=campaign,
                         metrics=metrics,
                         score=score,
                         daily_data=daily_data)

@app.route('/traffic/create', methods=['GET', 'POST'])
@login_required
def create_campaign():
    if request.method == 'POST':
        db = get_db()

        db.execute('''
            INSERT INTO campaigns (name, platform, budget, target_audience, ad_creative, user_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            request.form['name'],
            request.form['platform'],
            float(request.form['budget']),
            request.form['target_audience'],
            request.form['ad_creative'],
            session['user_id']
        ))

        db.commit()
        return redirect(url_for('traffic'))

    return render_template('create_campaign.html')

# ===== RANKING IA =====
@app.route('/ranking')
@login_required
def ranking():
    db = get_db()

    # Ranking de produtos por performance
    products = db.execute('''
        SELECT p.*,
               COALESCE(SUM(cp.revenue), 0) as total_revenue,
               COALESCE(SUM(cp.conversions), 0) as total_conversions,
               COALESCE(AVG(cp.conversion_rate), 0) as avg_conversion_rate,
               CASE
                   WHEN AVG(cp.conversion_rate) >= 5 THEN 'Excelente'
                   WHEN AVG(cp.conversion_rate) >= 2 THEN 'Bom'
                   WHEN AVG(cp.conversion_rate) >= 1 THEN 'Regular'
                   ELSE 'Ruim'
               END as performance_level
        FROM products p
        LEFT JOIN campaign_products cp ON p.id = cp.product_id
        GROUP BY p.id
        ORDER BY total_revenue DESC
    ''').fetchall()

    # Ranking de palavras-chave
    keywords = db.execute('''
        SELECT keyword,
               COUNT(*) as campaigns_count,
               AVG(ctr) as avg_ctr,
               AVG(conversion_rate) as avg_conversion_rate,
               SUM(revenue) as total_revenue
        FROM campaign_keywords ck
        JOIN campaigns c ON ck.campaign_id = c.id
        GROUP BY keyword
        HAVING campaigns_count >= 2
        ORDER BY total_revenue DESC
        LIMIT 20
    ''').fetchall()

    return render_template('ranking.html',
                         products=products,
                         keywords=keywords)

# ===== MARKETPLACES =====
@app.route('/marketplaces')
@login_required
def marketplaces():
    return render_template('marketplaces.html')

@app.route('/marketplaces/<platform>')
@login_required
def marketplace_detail(platform):
    db = get_db()

    # Configurações específicas da plataforma
    platform_config = db.execute(
        'SELECT * FROM marketplace_configs WHERE platform = ? AND user_id = ?',
        (platform, session['user_id'])
    ).fetchone()

    # Produtos desta plataforma
    products = db.execute('''
        SELECT * FROM marketplace_products
        WHERE platform = ? AND user_id = ?
        ORDER BY created_at DESC
    ''', (platform, session['user_id'])).fetchall()

    return render_template('marketplace_detail.html',
                         platform=platform,
                         config=platform_config,
                         products=products)

@app.route('/marketplaces/<platform>/sync', methods=['POST'])
@login_required
def sync_marketplace(platform):
    # Simula sincronização com API da plataforma
    # Na implementação real, aqui faria chamadas para APIs específicas

    return jsonify({
        'status': 'success',
        'message': f'Sincronização com {platform} iniciada',
        'products_updated': 15,
        'orders_imported': 8
    })

# ===== HUB DE INTEGRAÇÕES =====
@app.route('/integrations')
@login_required
def integrations():
    db = get_db()

    # Status das integrações do usuário
    integrations = db.execute('''
        SELECT platform, status, last_sync, access_token IS NOT NULL as connected
        FROM user_integrations
        WHERE user_id = ?
    ''', (session['user_id'],)).fetchall()

    # Plataformas disponíveis
    available_platforms = [
        'mercadolivre', 'amazon', 'magalu', 'shopee', 'americanas',
        'casasbahia', 'pontofrio', 'extra', 'submarino', 'tiktokshop',
        'facebook', 'google', 'bing'
    ]

    return render_template('integrations.html',
                         integrations=integrations,
                         available_platforms=available_platforms)

@app.route('/integrations/<platform>/connect')
@login_required
def connect_integration(platform):
    # OAuth flow para conectar plataforma
    oauth_url = f"https://oauth.{platform}.com/authorize?client_id=YOUR_CLIENT_ID&redirect_uri=YOUR_REDIRECT&scope=read_products,manage_orders"
    return redirect(oauth_url)

@app.route('/oauth/<platform>/callback')
def oauth_callback(platform):
    code = request.args.get('code')
    if code:
        # Trocar code por access_token
        # Salvar no banco

        db = get_db()
        db.execute('''
            INSERT OR REPLACE INTO user_integrations
            (user_id, platform, access_token, status)
            VALUES (?, ?, ?, 'connected')
        ''', (session['user_id'], platform, f'token_{code[:10]}'))
        db.commit()

    return redirect(url_for('integrations'))

# ===== API ENDPOINTS =====
@app.route('/api/campaigns')
@login_required
def api_campaigns():
    db = get_db()
    campaigns = db.execute('SELECT * FROM campaigns WHERE user_id = ?', (session['user_id'],)).fetchall()

    return jsonify([dict(campaign) for campaign in campaigns])

@app.route('/api/customers/segment/<segment>')
@login_required
def api_customers_by_segment(segment):
    db = get_db()

    # Query complexa para filtrar por segmento RFM
    customers = db.execute('''
        SELECT * FROM customers
        WHERE user_id = ?
        -- Adicionar lógica de segmentação baseada em RFM
        LIMIT 50
    ''', (session['user_id'],)).fetchall()

    return jsonify([dict(customer) for customer in customers])

@app.route('/api/analytics/overview')
@login_required
def api_analytics_overview():
    db = get_db()

    # Métricas gerais
    overview = {
        'total_campaigns': db.execute('SELECT COUNT(*) as count FROM campaigns WHERE user_id = ?', (session['user_id'],)).fetchone()['count'],
        'total_customers': db.execute('SELECT COUNT(*) as count FROM customers WHERE user_id = ?', (session['user_id'],)).fetchone()['count'],
        'total_revenue': db.execute('SELECT COALESCE(SUM(revenue), 0) as total FROM campaigns WHERE user_id = ?', (session['user_id'],)).fetchone()['total'],
        'avg_roi': db.execute('SELECT AVG(revenue * 100.0 / budget) as avg_roi FROM campaigns WHERE user_id = ? AND budget > 0', (session['user_id'],)).fetchone()['avg_roi'] or 0
    }

    return jsonify(overview)

if __name__ == '__main__':
    app.run(debug=True)
