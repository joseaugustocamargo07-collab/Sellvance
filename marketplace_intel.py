"""
Sellvance — Motor de Inteligência de Marketplaces
Análise competitiva, estoque, anúncios, devoluções, saúde da conta
"""

# ── Dados dos concorrentes (simulados com base em dados reais de mercado) ────

COMPETITORS = {
    'mercado_livre': [
        {'id': 'c1', 'name': 'ThermoBox BR',      'rating': 4.7, 'reviews': 2840, 'price_32l': 229.90, 'price_20l': 179.90, 'stock': 'ok',       'sponsored': True,  'badge': 'MercadoLíder Gold',   'shipping': 'grátis', 'fulfillment': True},
        {'id': 'c2', 'name': 'CoolMaster',          'rating': 4.5, 'reviews': 1230, 'price_32l': 199.90, 'price_20l': 159.90, 'stock': 'low',      'sponsored': True,  'badge': 'MercadoLíder',        'shipping': 'grátis', 'fulfillment': False},
        {'id': 'c3', 'name': 'Glacial Coolers',     'rating': 4.3, 'reviews':  890, 'price_32l': 189.90, 'price_20l': 149.90, 'stock': 'ok',       'sponsored': False, 'badge': None,                  'shipping': 'R$15',   'fulfillment': False},
        {'id': 'c4', 'name': 'FrezzPack',           'rating': 3.9, 'reviews':  420, 'price_32l': 169.90, 'price_20l': 129.90, 'stock': 'critical', 'sponsored': False, 'badge': None,                  'shipping': 'R$18',   'fulfillment': False},
        {'id': 'c5', 'name': 'AlpineBox',           'rating': 4.6, 'reviews': 1680, 'price_32l': 249.90, 'price_20l': 199.90, 'stock': 'ok',       'sponsored': True,  'badge': 'MercadoLíder Platinum','shipping': 'grátis','fulfillment': True},
    ],
    'amazon': [
        {'id': 'a1', 'name': 'Coleman Brazil',     'rating': 4.8, 'reviews': 5240, 'price_32l': 259.90, 'price_20l': 199.90, 'stock': 'ok',       'sponsored': True,  'badge': 'Amazon Choice',       'shipping': 'Prime',  'fulfillment': True},
        {'id': 'a2', 'name': 'IceBreaker',          'rating': 4.4, 'reviews':  980, 'price_32l': 209.90, 'price_20l': 169.90, 'stock': 'ok',       'sponsored': True,  'badge': None,                  'shipping': 'Prime',  'fulfillment': True},
        {'id': 'a3', 'name': 'ThermoBox BR',        'rating': 4.6, 'reviews': 1890, 'price_32l': 224.90, 'price_20l': 174.90, 'stock': 'low',      'sponsored': False, 'badge': None,                  'shipping': 'grátis', 'fulfillment': False},
        {'id': 'a4', 'name': 'PolarBox',            'rating': 4.2, 'reviews':  340, 'price_32l': 184.90, 'price_20l': 144.90, 'stock': 'critical', 'sponsored': False, 'badge': None,                  'shipping': 'R$20',   'fulfillment': False},
    ],
    'tiktok_shop': [
        {'id': 't1', 'name': 'IceCool BR',         'rating': 4.6, 'reviews':  820, 'price_32l': 189.90, 'price_20l': 149.90, 'stock': 'ok',       'sponsored': True,  'badge': 'Top Seller',          'shipping': 'grátis', 'fulfillment': False},
        {'id': 't2', 'name': 'FrezBox Oficial',     'rating': 4.3, 'reviews':  340, 'price_32l': 179.90, 'price_20l': 139.90, 'stock': 'low',      'sponsored': True,  'badge': None,                  'shipping': 'grátis', 'fulfillment': False},
        {'id': 't3', 'name': 'CoolVibes',           'rating': 4.1, 'reviews':  180, 'price_32l': 169.90, 'price_20l': 134.90, 'stock': 'ok',       'sponsored': False, 'badge': None,                  'shipping': 'R$12',   'fulfillment': False},
    ],
}

MY_PRODUCTS = {
    'mercado_livre': {'price_32l': 219.90, 'price_20l': 189.90, 'rating': 4.8, 'reviews': 3240, 'stock_32l': 48, 'stock_20l': 9,  'badge': 'MercadoLíder Gold', 'fulfillment': True},
    'amazon':        {'price_32l': 219.90, 'price_20l': 189.90, 'rating': 4.7, 'reviews': 2180, 'stock_32l': 22, 'stock_20l': 0,  'badge': None,                 'fulfillment': False},
    'tiktok_shop':   {'price_32l': 189.90, 'price_20l': 159.90, 'rating': 4.5, 'reviews':  680, 'stock_32l': 67, 'stock_20l': 3,  'badge': 'Top Seller',         'fulfillment': False},
}

MP_ADS_DATA = {
    'mercado_livre': [
        {'name': 'Cooler 32L Azul — Product Ads',  'type': 'product_ads', 'spend': 1240, 'revenue': 8680, 'clicks': 3420, 'impressions': 84000, 'conversions': 89, 'acos': 14.3, 'status': 'active'},
        {'name': 'Cooler 20L — Product Ads',        'type': 'product_ads', 'spend':  680, 'revenue': 2380, 'clicks': 2180, 'impressions': 62000, 'conversions': 25, 'acos': 28.6, 'status': 'active'},
        {'name': 'Kit Cooler — Display Banner',     'type': 'display',     'spend':  420, 'revenue': 1260, 'clicks':  840, 'impressions': 38000, 'conversions': 13, 'acos': 33.3, 'status': 'active'},
        {'name': 'Cooler Verão — Sponsored Brand',  'type': 'sponsored',   'spend':  890, 'revenue': 5340, 'clicks': 2940, 'impressions': 71000, 'conversions': 56, 'acos': 16.7, 'status': 'active'},
    ],
    'amazon': [
        {'name': 'Cooler 32L — Sponsored Products', 'type': 'sponsored',   'spend':  980, 'revenue': 6860, 'clicks': 2840, 'impressions': 68000, 'conversions': 70, 'acos': 14.3, 'status': 'active'},
        {'name': 'Cooler 20L — Sponsored Products', 'type': 'sponsored',   'spend':  340, 'revenue':  680, 'clicks':  980, 'impressions': 31000, 'conversions':  7, 'acos': 50.0, 'status': 'active'},
        {'name': 'Brand Store — Headline Ads',      'type': 'headline',    'spend':  560, 'revenue': 3360, 'clicks': 1680, 'impressions': 45000, 'conversions': 35, 'acos': 16.7, 'status': 'active'},
    ],
    'tiktok_shop': [
        {'name': 'Cooler 32L — Shop Ads',           'type': 'shop_ads',    'spend':  780, 'revenue': 5460, 'clicks': 4200, 'impressions': 98000, 'conversions': 57, 'acos': 14.3, 'status': 'active'},
        {'name': 'Cooler Verão — Video Shopping',   'type': 'video',       'spend':  490, 'revenue': 3430, 'clicks': 2940, 'impressions': 72000, 'conversions': 36, 'acos': 14.3, 'status': 'active'},
        {'name': 'Afiliados — Comissão 8%',         'type': 'affiliate',   'spend':  340, 'revenue': 1020, 'clicks': 1420, 'impressions': 34000, 'conversions': 11, 'acos': 33.3, 'status': 'active'},
    ],
}

RETURNS_DATA = {
    'mercado_livre': {
        'total_orders': 312, 'total_returns': 14, 'return_rate': 4.5,
        'reasons': [
            {'reason': 'Produto diferente do anunciado', 'count': 4, 'pct': 28.6},
            {'reason': 'Defeito de fabricação',          'count': 3, 'pct': 21.4},
            {'reason': 'Arrependimento de compra',       'count': 3, 'pct': 21.4},
            {'reason': 'Chegou danificado',              'count': 2, 'pct': 14.3},
            {'reason': 'Tamanho diferente do esperado',  'count': 2, 'pct': 14.3},
        ],
        'avg_resolution_days': 3.2,
        'refunded_revenue': 2660.40,
        'trend': 'down',  # melhorando
    },
    'amazon': {
        'total_orders': 184, 'total_returns': 12, 'return_rate': 6.5,
        'reasons': [
            {'reason': 'Produto diferente do anunciado', 'count': 5, 'pct': 41.7},
            {'reason': 'Defeito de fabricação',          'count': 3, 'pct': 25.0},
            {'reason': 'Chegou danificado',              'count': 2, 'pct': 16.7},
            {'reason': 'Arrependimento de compra',       'count': 2, 'pct': 16.7},
        ],
        'avg_resolution_days': 4.8,
        'refunded_revenue': 2279.40,
        'trend': 'up',  # piorando
    },
    'tiktok_shop': {
        'total_orders': 241, 'total_returns': 8, 'return_rate': 3.3,
        'reasons': [
            {'reason': 'Arrependimento de compra',       'count': 3, 'pct': 37.5},
            {'reason': 'Produto diferente do anunciado', 'count': 2, 'pct': 25.0},
            {'reason': 'Defeito de fabricação',          'count': 2, 'pct': 25.0},
            {'reason': 'Chegou danificado',              'count': 1, 'pct': 12.5},
        ],
        'avg_resolution_days': 2.1,
        'refunded_revenue': 1519.20,
        'trend': 'stable',
    },
}

ACCOUNT_HEALTH = {
    'mercado_livre': {
        'score': 92, 'level': 'MercadoLíder Gold', 'color': '#f59e0b',
        'metrics': {
            'Reputação': {'val': '98%', 'ok': True},
            'Envio no prazo': {'val': '97%', 'ok': True},
            'Reclamações': {'val': '1.2%', 'ok': True},
            'Cancelamentos': {'val': '0.8%', 'ok': True},
            'Avaliação média': {'val': '4.8 ⭐', 'ok': True},
            'Catálogo ativo': {'val': '6 SKUs', 'ok': True},
        },
        'alerts': [],
    },
    'amazon': {
        'score': 74, 'level': 'Seller Standard', 'color': '#ff9900',
        'metrics': {
            'ODR (Defeito)':   {'val': '2.8%', 'ok': False},
            'Cancelamentos':   {'val': '1.9%', 'ok': False},
            'Envio no prazo':  {'val': '94%',  'ok': True},
            'Avaliação média': {'val': '4.7 ⭐','ok': True},
            'A-to-Z Claims':   {'val': '0.3%', 'ok': True},
            'Fulfillment':     {'val': 'FBM',   'ok': False},
        },
        'alerts': [
            '⚠️ ODR acima de 1% — risco de suspensão da conta',
            '⚠️ Habilitar FBA pode melhorar score e conversão em ~30%',
            '⚠️ Taxa de cancelamento alta — revisar estoque disponível',
        ],
    },
    'tiktok_shop': {
        'score': 88, 'level': 'Top Seller', 'color': '#ff0050',
        'metrics': {
            'Taxa de conclusão': {'val': '96%', 'ok': True},
            'Avaliação média':   {'val': '4.5 ⭐','ok': True},
            'Envio em 48h':      {'val': '91%', 'ok': True},
            'Reclamações':       {'val': '2.1%', 'ok': True},
            'Afiliados ativos':  {'val': '23',   'ok': True},
            'Vídeos ao vivo':    {'val': '0',    'ok': False},
        },
        'alerts': [
            '💡 Lives de vendas podem aumentar conversão em 3-5x no TikTok Shop',
        ],
    },
}


def analyze_competitive_position(marketplace):
    """Analisa posição competitiva e gera recomendações."""
    my   = MY_PRODUCTS.get(marketplace, {})
    comp = COMPETITORS.get(marketplace, [])
    recs = []
    opportunities = []

    my_32l = my.get('price_32l', 0)
    my_20l = my.get('price_20l', 0)

    # Concorrentes com estoque crítico = oportunidade
    critical = [c for c in comp if c['stock'] in ('critical', 'out')]
    if critical:
        names = ', '.join(c['name'] for c in critical)
        opportunities.append({
            'type': 'stock_gap',
            'icon': '📦',
            'title': f"{len(critical)} concorrente(s) com estoque crítico",
            'text': f"{names} — Aumente budget de anúncios agora para capturar demanda",
            'impact': 'Alto',
            'urgency': 'Imediata',
        })

    # Análise de preço
    prices_32l = [c['price_32l'] for c in comp]
    avg_price  = sum(prices_32l) / len(prices_32l) if prices_32l else my_32l
    min_price  = min(prices_32l) if prices_32l else my_32l
    max_price  = max(prices_32l) if prices_32l else my_32l

    if my_32l > avg_price * 1.1:
        recs.append({
            'type': 'price',
            'icon': '💰',
            'title': 'Preço acima da média de mercado',
            'text': f"Seu preço R${my_32l} vs média R${avg_price:.0f}. Considere reduzir ou reforçar percepção de valor com bundle.",
            'priority': 'alta',
        })
    elif my_32l < min_price:
        recs.append({
            'type': 'price',
            'icon': '🏷️',
            'title': 'Você tem o menor preço — oportunidade de margem',
            'text': f"Concorrente mais barato cobra R${min_price}. Você pode subir R${my_32l} até R${min_price - 5:.2f} sem perder competitividade.",
            'priority': 'media',
        })

    # Análise de avaliações
    my_rating   = my.get('rating', 0)
    my_reviews  = my.get('reviews', 0)
    best_rating = max((c['rating'] for c in comp), default=0)
    most_reviews = max((c['reviews'] for c in comp), default=0)

    if my_rating >= best_rating:
        opportunities.append({
            'type': 'rating',
            'icon': '⭐',
            'title': f"Melhor avaliação do marketplace ({my_rating})",
            'text': "Destaque isso no título e imagens — aumenta CTR em até 15%",
            'impact': 'Médio',
            'urgency': 'Esta semana',
        })

    # Concorrentes sem fulfillment = desvantagem deles
    no_fulfillment = [c for c in comp if not c['fulfillment'] and my.get('fulfillment')]
    if no_fulfillment and marketplace == 'mercado_livre':
        opportunities.append({
            'type': 'fulfillment',
            'icon': '🚚',
            'title': f"{len(no_fulfillment)} concorrentes sem Full',",
            'text': "Seu Fulfillment é vantagem competitiva — destaque 'Entrega Full' nos anúncios",
            'impact': 'Alto',
            'urgency': 'Esta semana',
        })

    return {
        'recommendations': recs,
        'opportunities': opportunities,
        'avg_price_32l': round(avg_price, 2),
        'min_price_32l': min_price,
        'max_price_32l': max_price,
        'price_position': 'acima' if my_32l > avg_price else 'abaixo' if my_32l < avg_price * 0.95 else 'alinhado',
    }


def analyze_mp_ads(marketplace):
    """Analisa anúncios internos do marketplace."""
    ads = MP_ADS_DATA.get(marketplace, [])
    results = []
    for ad in ads:
        spend   = ad['spend']
        revenue = ad['revenue']
        clicks  = ad['clicks'] or 1
        impr    = ad['impressions'] or 1
        conv    = ad['conversions'] or 0
        acos    = ad['acos']  # Advertising Cost of Sale %

        roas = round(revenue / spend, 1) if spend > 0 else 0
        ctr  = round(clicks / impr * 100, 2)
        cpc  = round(spend / clicks, 2)
        cpa  = round(spend / conv, 2) if conv > 0 else 0

        # Score do anúncio
        if acos <= 15:
            score = 90; action = 'scale'; label = '🚀 Escalar'
        elif acos <= 25:
            score = 70; action = 'optimize'; label = '⚠️ Otimizar'
        elif acos <= 35:
            score = 50; action = 'watch'; label = '👀 Monitorar'
        else:
            score = 30; action = 'pause'; label = '🔴 Pausar'

        results.append({**ad, 'roas': roas, 'ctr': ctr, 'cpc': cpc, 'cpa': cpa,
                        'score': score, 'action': action, 'action_label': label})
    return sorted(results, key=lambda x: -x['score'])


def get_keyword_opportunities(marketplace):
    """Palavras-chave com alto volume e baixa concorrência."""
    kws = {
        'mercado_livre': [
            {'kw': 'cooler 32 litros',      'volume': 18400, 'competition': 'alta',  'your_pos': 2,  'cpc_est': 1.20, 'opportunity': 'médio'},
            {'kw': 'caixa térmica camping', 'volume': 12800, 'competition': 'média', 'your_pos': 4,  'cpc_est': 0.85, 'opportunity': 'alto'},
            {'kw': 'cooler praia',           'volume': 9200,  'competition': 'média', 'your_pos': 6,  'cpc_est': 0.72, 'opportunity': 'alto'},
            {'kw': 'cooler 20 litros',       'volume': 7600,  'competition': 'baixa', 'your_pos': 8,  'cpc_est': 0.60, 'opportunity': 'muito alto'},
            {'kw': 'caixa de isopor grande', 'volume': 6400,  'competition': 'baixa', 'your_pos': 12, 'cpc_est': 0.45, 'opportunity': 'muito alto'},
            {'kw': 'bolsa térmica 30l',      'volume': 4800,  'competition': 'baixa', 'your_pos': None,'cpc_est': 0.40, 'opportunity': 'muito alto'},
        ],
        'amazon': [
            {'kw': 'cooler box 32l',         'volume': 8400,  'competition': 'alta',  'your_pos': 3,  'cpc_est': 1.40, 'opportunity': 'médio'},
            {'kw': 'caixa termica camping',  'volume': 5600,  'competition': 'média', 'your_pos': 5,  'cpc_est': 0.95, 'opportunity': 'alto'},
            {'kw': 'cooler beach brazil',    'volume': 3200,  'competition': 'baixa', 'your_pos': 9,  'cpc_est': 0.65, 'opportunity': 'muito alto'},
        ],
        'tiktok_shop': [
            {'kw': '#coolerverão',           'volume': 42000, 'competition': 'média', 'your_pos': 4,  'cpc_est': 0.30, 'opportunity': 'muito alto'},
            {'kw': '#caixatermica',          'volume': 28000, 'competition': 'baixa', 'your_pos': 7,  'cpc_est': 0.25, 'opportunity': 'muito alto'},
            {'kw': '#camping',              'volume': 184000, 'competition': 'alta',  'your_pos': None,'cpc_est': 0.50, 'opportunity': 'alto'},
        ],
    }
    return kws.get(marketplace, [])


# ═══════════════════════════════════════════════════════════════
# DB-FIRST FUNCTIONS (real API data with demo fallback)
# ═══════════════════════════════════════════════════════════════

def get_my_products_live(org_id, marketplace):
    """Returns product data from DB if available, else demo data."""
    try:
        from database import get_db
        db = get_db()
        products = db.execute(
            "SELECT * FROM mp_products WHERE org_id=? AND platform=? AND status='active' ORDER BY sold_qty DESC",
            (org_id, marketplace)
        ).fetchall()
        db.close()

        if products and len(products) > 0:
            # Transform to the format templates expect
            total_stock = sum(p['stock_qty'] or 0 for p in products)
            total_sold = sum(p['sold_qty'] or 0 for p in products)
            avg_price = sum(p['price'] or 0 for p in products) / len(products) if products else 0
            avg_rating = sum(p['rating'] or 0 for p in products) / len(products) if products else 0
            total_reviews = sum(p['reviews'] or 0 for p in products)

            return {
                'price_32l': products[0]['price'] if len(products) > 0 else 0,
                'price_20l': products[1]['price'] if len(products) > 1 else 0,
                'rating': round(avg_rating, 1) if avg_rating > 0 else 4.5,
                'reviews': total_reviews,
                'stock_32l': products[0]['stock_qty'] if len(products) > 0 else 0,
                'stock_20l': products[1]['stock_qty'] if len(products) > 1 else 0,
                'badge': None,
                'fulfillment': False,
                '_live': True,
                '_products': [dict(p) for p in products],
                '_total_stock': total_stock,
                '_total_sold': total_sold,
            }
    except Exception as e:
        print(f"[marketplace_intel] DB error for products: {e}")

    return MY_PRODUCTS.get(marketplace, {})


def get_account_health_live(org_id, marketplace):
    """Returns account health from DB if available, else demo data."""
    try:
        from database import get_db
        import json as _json
        db = get_db()
        row = db.execute(
            "SELECT * FROM mp_account_health WHERE org_id=? AND platform=?",
            (org_id, marketplace)
        ).fetchone()
        db.close()

        if row:
            raw_metrics = _json.loads(row['metrics_json'] or '{}')
            # Convert to template format: {"label": {"ok": bool, "val": str}}
            formatted = {}
            thresholds = {
                'reputacao': lambda v: _parse_pct(v) >= 80,
                'vendas_completas': lambda v: True,
                'reclamacoes': lambda v: _parse_pct(v) <= 3,
                'atrasos': lambda v: _parse_pct(v) <= 5,
                'cancelamentos': lambda v: _parse_pct(v) <= 3,
            }
            label_map = {
                'reputacao': 'Reputação',
                'vendas_completas': 'Vendas completas',
                'reclamacoes': 'Reclamações',
                'atrasos': 'Envio no prazo',
                'cancelamentos': 'Cancelamentos',
            }
            for key, val in raw_metrics.items():
                check = thresholds.get(key, lambda v: True)
                try:
                    ok = check(val)
                except Exception:
                    ok = True
                formatted[label_map.get(key, key)] = {'ok': ok, 'val': str(val)}

            return {
                'score': row['score'],
                'level': row['level'],
                'metrics': formatted,
                'alerts': _json.loads(row['alerts_json'] or '[]'),
                '_live': True,
            }
    except Exception as e:
        print(f"[marketplace_intel] DB error for health: {e}")

    return ACCOUNT_HEALTH.get(marketplace, {'score': 0, 'metrics': {}, 'alerts': []})


def get_returns_live(org_id, marketplace):
    """Returns return data from DB if available, else demo data."""
    try:
        from database import get_db
        import json as _json
        db = get_db()
        row = db.execute(
            "SELECT * FROM mp_returns WHERE org_id=? AND platform=?",
            (org_id, marketplace)
        ).fetchone()
        db.close()

        if row:
            return {
                'total_orders': row['total_orders'],
                'total_returns': row['total_returns'],
                'return_rate': row['return_rate'],
                'reasons': _json.loads(row['reasons_json'] or '[]'),
                'avg_resolution_days': row['avg_resolution_days'],
                'refunded_revenue': row['refunded_revenue'],
                'trend': row['trend'],
                '_live': True,
            }
    except Exception as e:
        print(f"[marketplace_intel] DB error for returns: {e}")

    return RETURNS_DATA.get(marketplace, {})


def get_mp_totals_live(org_id):
    """Returns revenue/orders totals per marketplace from real orders."""
    try:
        from database import get_db
        db = get_db()
        mp_totals = {}
        for mp_id in ['mercado_livre', 'amazon', 'tiktok_shop']:
            row = db.execute(
                "SELECT COALESCE(SUM(revenue), 0) as revenue, COUNT(*) as orders FROM orders WHERE org_id=? AND marketplace=?",
                (org_id, mp_id)
            ).fetchone()
            mp_totals[mp_id] = {'revenue': row['revenue'], 'orders': row['orders']}
        db.close()
        return mp_totals
    except Exception:
        return {}


def is_platform_synced(org_id, marketplace):
    """Check if we have real synced data for this platform."""
    try:
        from database import get_db
        db = get_db()
        row = db.execute(
            "SELECT COUNT(*) as cnt FROM sync_log WHERE org_id=? AND platform=? AND status='success'",
            (org_id, marketplace)
        ).fetchone()
        db.close()
        return row['cnt'] > 0
    except Exception:
        return False


def get_ads_live(org_id, marketplace):
    """Returns promoted listings data. Checks mp_ads first, then falls back to
    mp_products with promoted listing types (gold_pro, gold_special, gold_premium)."""
    try:
        from database import get_db
        db = get_db()

        # First try mp_ads table (has spend/revenue data if populated)
        rows = db.execute(
            "SELECT * FROM mp_ads WHERE org_id=? AND platform=? ORDER BY spend DESC",
            (org_id, marketplace)
        ).fetchall()

        if rows:
            ads_list = []
            total_spend = 0
            total_revenue = 0
            for r in rows:
                ad = dict(r)
                ads_list.append(ad)
                total_spend += (ad.get('spend') or 0)
                total_revenue += (ad.get('revenue') or 0)
            db.close()
            return {
                'ads': ads_list,
                'total_spend': total_spend,
                'total_revenue': total_revenue,
                'roas': round(total_revenue / total_spend, 1) if total_spend > 0 else 0,
                '_live': True,
            }

        # Fallback: get promoted products from mp_products
        promoted_types = ('gold_pro', 'gold_special', 'gold_premium')
        placeholders = ','.join('?' for _ in promoted_types)
        rows = db.execute(
            f"SELECT * FROM mp_products WHERE org_id=? AND platform=? AND listing_type IN ({placeholders}) ORDER BY sold_qty DESC",
            (org_id, marketplace) + promoted_types
        ).fetchall()
        db.close()

        if rows:
            ads_list = []
            for r in rows:
                p = dict(r)
                revenue = (p.get('price', 0) or 0) * (p.get('sold_qty', 0) or 0)
                listing_label = {
                    'gold_pro': 'Premium',
                    'gold_special': 'Clássico',
                    'gold_premium': 'Premium',
                }.get(p.get('listing_type', ''), 'Promovido')
                ads_list.append(p | {'_revenue_estimated': revenue, '_listing_label': listing_label})

            return {
                'ads': ads_list,
                '_from_products': True,
                '_live': True,
            }
    except Exception as e:
        print(f"[marketplace_intel] DB error for ads: {e}")

    return None


def get_real_orders_totals(org_id, marketplace, date_start='', date_end=''):
    """Returns revenue/orders from REAL synced orders only (with external_id)."""
    try:
        from database import get_db
        db = get_db()
        sql = "SELECT COALESCE(SUM(revenue), 0) as revenue, COUNT(*) as orders FROM orders WHERE org_id=? AND marketplace=? AND external_id IS NOT NULL AND external_id != ''"
        params = [org_id, marketplace]
        if date_start:
            sql += " AND date(ordered_at) >= date(?)"
            params.append(date_start)
        if date_end:
            sql += " AND date(ordered_at) <= date(?)"
            params.append(date_end)
        row = db.execute(sql, params).fetchone()
        db.close()
        return {'revenue': row['revenue'], 'orders': row['orders']}
    except Exception as e:
        print(f"[marketplace_intel] DB error for real orders: {e}")
        return {'revenue': 0, 'orders': 0}


def get_real_products_list(org_id, marketplace):
    """Returns list of real synced products for display."""
    try:
        from database import get_db
        db = get_db()
        rows = db.execute(
            "SELECT * FROM mp_products WHERE org_id=? AND platform=? ORDER BY sold_qty DESC",
            (org_id, marketplace)
        ).fetchall()
        db.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[marketplace_intel] DB error for products list: {e}")
        return []


def get_ads_from_campaigns(org_id, marketplace):
    """Returns ad campaigns from ad_campaigns table for this marketplace/platform."""
    try:
        from database import get_db
        db = get_db()
        rows = db.execute(
            "SELECT * FROM ad_campaigns WHERE org_id=? AND platform=? ORDER BY spend DESC LIMIT 10",
            (org_id, marketplace)
        ).fetchall()
        db.close()
        if rows:
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"[marketplace_intel] DB error for ad campaigns: {e}")
    return []


def get_keywords_from_products(org_id, marketplace):
    """Generate keyword opportunities from real product titles."""
    try:
        from database import get_db
        db = get_db()
        rows = db.execute(
            "SELECT title, price, sold_qty FROM mp_products WHERE org_id=? AND platform=? AND status='active'",
            (org_id, marketplace)
        ).fetchall()
        db.close()

        if not rows:
            return []

        # Extract meaningful keywords from product titles
        stop_words = {'para', 'ou', 'de', 'do', 'da', 'dos', 'das', 'um', 'uma', 'e', 'com', 'cor', 'no', 'na',
                      'em', 'os', 'as', 'que', 'por', 'se', 'a', 'o', 'ao'}
        word_freq = {}
        bigrams = {}
        trigrams = {}

        for row in rows:
            title = dict(row).get('title', '')
            words = [w.lower().strip() for w in title.split() if len(w) > 2 and w.lower() not in stop_words]

            for w in words:
                word_freq[w] = word_freq.get(w, 0) + 1

            for i in range(len(words) - 1):
                bg = f"{words[i]} {words[i+1]}"
                bigrams[bg] = bigrams.get(bg, 0) + 1

            for i in range(len(words) - 2):
                tg = f"{words[i]} {words[i+1]} {words[i+2]}"
                trigrams[tg] = trigrams.get(tg, 0) + 1

        # Build keyword list from most common phrases
        keywords = []
        seen = set()

        # Trigrams first (most specific)
        for phrase, count in sorted(trigrams.items(), key=lambda x: -x[1])[:4]:
            if phrase not in seen:
                keywords.append({
                    'kw': phrase,
                    'volume': 5000 + count * 2000,
                    'competition': 'media' if count > 1 else 'baixa',
                    'your_pos': min(count * 2, 10),
                    'cpc_est': round(0.5 + count * 0.15, 2),
                    'opportunity': 'muito alto' if count > 2 else 'alto',
                })
                seen.add(phrase)

        # Bigrams
        for phrase, count in sorted(bigrams.items(), key=lambda x: -x[1])[:4]:
            if phrase not in seen and not any(phrase in s for s in seen):
                keywords.append({
                    'kw': phrase,
                    'volume': 8000 + count * 3000,
                    'competition': 'alta' if count > 2 else 'media',
                    'your_pos': min(count, 8),
                    'cpc_est': round(0.6 + count * 0.2, 2),
                    'opportunity': 'alto' if count > 1 else 'medio',
                })
                seen.add(phrase)

        # Single high-freq words
        for word, count in sorted(word_freq.items(), key=lambda x: -x[1])[:3]:
            if word not in seen and len(word) > 3:
                keywords.append({
                    'kw': word,
                    'volume': 12000 + count * 4000,
                    'competition': 'alta',
                    'your_pos': min(count + 1, 5),
                    'cpc_est': round(0.8 + count * 0.25, 2),
                    'opportunity': 'medio',
                })
                seen.add(word)

        return keywords[:8] if keywords else []

    except Exception as e:
        print(f"[marketplace_intel] Error generating keywords: {e}")
        return []


def search_ml_competitors(org_id, marketplace, token=None):
    """Search ML for competitors selling similar products using authenticated API."""
    import json
    import urllib.request
    try:
        from database import get_db
        db = get_db()

        # Get user's product categories
        rows = db.execute(
            "SELECT category, title, price FROM mp_products WHERE org_id=? AND platform=? AND status='active' ORDER BY sold_qty DESC LIMIT 3",
            (org_id, marketplace)
        ).fetchall()

        # Get our seller ID
        integration = db.execute(
            "SELECT account_id FROM integrations WHERE org_id=? AND platform=?",
            (org_id, marketplace)
        ).fetchone()
        db.close()

        if not rows:
            return []

        our_seller_id = str(dict(integration).get('account_id', '')) if integration else ''

        # Get ML token for authenticated API calls
        if not token:
            from sync_base import get_valid_token
            token = get_valid_token(org_id, marketplace)

        if not token:
            print("[competitors] No valid ML token")
            return []

        # Use public API (no auth) - more reliable for search
        headers = {
            'User-Agent': 'Sellvance/1.0',
            'Accept': 'application/json',
        }

        # Search by category (most reliable method)
        top_product = dict(rows[0])
        category = top_product.get('category', '')

        all_items = []

        # Try category search first
        if category:
            try:
                url = f"https://api.mercadolibre.com/sites/MLB/search?category={category}&limit=20&sort=sold_quantity_desc"
                req = urllib.request.Request(url, headers=headers)
                resp = json.loads(urllib.request.urlopen(req, timeout=15).read())
                all_items = resp.get('results', [])
                print(f"[competitors] Found {len(all_items)} items in category {category}")
            except Exception as e:
                print(f"[competitors] Category search error: {e}")

        # Fallback: search by keywords from title
        if not all_items:
            try:
                import re
                title = top_product.get('title', '')
                # Extract key terms
                words = re.sub(r'[^a-zA-Z\u00C0-\u024F\s]', ' ', title).split()
                search_words = [w for w in words if len(w) > 3 and w.lower() not in ('para', 'preto', 'branco', 'azul')][:3]
                query = '+'.join(search_words)
                url = f"https://api.mercadolibre.com/sites/MLB/search?q={query}&limit=20&sort=sold_quantity_desc"
                req = urllib.request.Request(url, headers=headers)
                resp = json.loads(urllib.request.urlopen(req, timeout=15).read())
                all_items = resp.get('results', [])
                print(f"[competitors] Found {len(all_items)} items by keyword search")
            except Exception as e:
                print(f"[competitors] Keyword search error: {e}")

        if not all_items:
            return []

        # Group by seller
        sellers = {}
        our_prices = [dict(r).get('price', 0) for r in rows]
        our_avg_price = sum(our_prices) / len(our_prices) if our_prices else 0

        for item in all_items:
            seller = item.get('seller', {})
            seller_id = str(seller.get('id', ''))

            # Skip ourselves
            if seller_id == our_seller_id or not seller_id:
                continue

            if seller_id not in sellers:
                rep = seller.get('seller_reputation', {})
                trans = rep.get('transactions', {})
                ratings = trans.get('ratings', {})
                positive_pct = ratings.get('positive', 0) or 0

                power = rep.get('power_seller_status') or ''
                badge_map = {
                    'platinum': 'MercadoLider Platinum',
                    'gold': 'MercadoLider Gold',
                    'silver': 'MercadoLider',
                    '': 'Seller padrao',
                }

                sellers[seller_id] = {
                    'name': seller.get('nickname', 'Vendedor'),
                    'rating': round(positive_pct * 5, 1),
                    'reviews': trans.get('completed', 0) or 0,
                    'price_32l': item.get('price', 0),
                    'price_20l': round(item.get('price', 0) * 0.85, 2),
                    'stock_32l': item.get('available_quantity', 0),
                    'stock_20l': 0,
                    'badge': badge_map.get(power, power or 'Seller padrao'),
                    'fulfillment': item.get('shipping', {}).get('logistic_type') == 'fulfillment',
                    'sponsored': item.get('listing_type_id', '') in ('gold_pro', 'gold_premium'),
                    'stock': 'normal' if item.get('available_quantity', 0) > 10 else 'critical' if item.get('available_quantity', 0) > 0 else 'out',
                    'sold_qty': item.get('sold_quantity', 0),
                }
            else:
                # Update with additional item data (average prices)
                existing = sellers[seller_id]
                existing['stock_20l'] = item.get('available_quantity', 0)
                existing['price_20l'] = item.get('price', existing['price_20l'])

        # Sort by sales volume, take top 6
        competitors = sorted(sellers.values(), key=lambda x: -(x.get('sold_qty', 0) or 0))[:6]
        print(f"[competitors] Returning {len(competitors)} competitors")
        return competitors

    except Exception as e:
        print(f"[marketplace_intel] Error searching competitors: {e}")
        import traceback
        traceback.print_exc()
        return []


def compute_health_score(metrics):
    """Compute a meaningful health score from ML account metrics."""
    score = 70  # Base score for having a connected account

    if not metrics:
        return score

    # Parse metrics
    claims_pct = float(str(metrics.get('reclamacoes', '0')).replace('%', '') or 0)
    delays_pct = float(str(metrics.get('atrasos', '0')).replace('%', '') or 0)
    cancellations_pct = float(str(metrics.get('cancelamentos', '0')).replace('%', '') or 0)
    completed = int(str(metrics.get('vendas_completas', '0')) or 0)

    # Add points for completed sales
    if completed >= 50:
        score += 15
    elif completed >= 20:
        score += 10
    elif completed >= 10:
        score += 5

    # Subtract for claims
    if claims_pct == 0:
        score += 5
    elif claims_pct > 3:
        score -= 10

    # Subtract for delays
    if delays_pct == 0:
        score += 5
    elif delays_pct > 5:
        score -= 10

    # Subtract for cancellations
    if cancellations_pct == 0:
        score += 5
    elif cancellations_pct > 3:
        score -= 10

    return max(0, min(100, score))
