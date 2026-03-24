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
        {'id': 'a1', 'name': 'Mor Caixas Térmicas',  'rating': 4.6, 'reviews': 3820, 'price_32l': 89.90,  'price_20l': 69.90,  'stock': 'ok',       'sponsored': True,  'badge': 'Amazon Choice',       'shipping': 'Prime',  'fulfillment': True},
        {'id': 'a2', 'name': 'Soprano Térmica',      'rating': 4.3, 'reviews': 1450, 'price_32l': 79.90,  'price_20l': 59.90,  'stock': 'ok',       'sponsored': True,  'badge': None,                  'shipping': 'Prime',  'fulfillment': True},
        {'id': 'a3', 'name': 'Invicta Cooler',             'rating': 4.5, 'reviews': 2100, 'price_32l': 94.90,  'price_20l': 74.90,  'stock': 'ok',       'sponsored': False, 'badge': None,                  'shipping': 'Prime',  'fulfillment': True},
        {'id': 'a4', 'name': 'BelFix Térmica',       'rating': 4.1, 'reviews':  680, 'price_32l': 67.90,  'price_20l': 49.90,  'stock': 'low',      'sponsored': False, 'badge': None,                  'shipping': 'grátis', 'fulfillment': False},
        {'id': 'a5', 'name': 'Obba Térmica',         'rating': 4.0, 'reviews':  290, 'price_32l': 72.90,  'price_20l': 54.90,  'stock': 'ok',       'sponsored': False, 'badge': None,                  'shipping': 'R$12',   'fulfillment': False},
    ],
    'tiktok_shop': [
        {'id': 't1', 'name': 'IceCool BR',         'rating': 4.6, 'reviews':  820, 'price_32l': 189.90, 'price_20l': 149.90, 'stock': 'ok',       'sponsored': True,  'badge': 'Top Seller',          'shipping': 'grátis', 'fulfillment': False},
        {'id': 't2', 'name': 'FrezBox Oficial',     'rating': 4.3, 'reviews':  340, 'price_32l': 179.90, 'price_20l': 139.90, 'stock': 'low',      'sponsored': True,  'badge': None,                  'shipping': 'grátis', 'fulfillment': False},
        {'id': 't3', 'name': 'CoolVibes',           'rating': 4.1, 'reviews':  180, 'price_32l': 169.90, 'price_20l': 134.90, 'stock': 'ok',       'sponsored': False, 'badge': None,                  'shipping': 'R$12',   'fulfillment': False},
    ],

    'shopee': [
        {'name': 'Caixa Térmica 32L — Shopee Ads', 'type': 'shop_ads', 'spend': 380, 'revenue': 4820, 'clicks': 2100, 'impressions': 58000, 'conversions': 66, 'acos': 7.9, 'status': 'active', 'price': 72.90, 'stock': 320},
        {'name': 'Caixa Térmica 20L — Shopee Ads', 'type': 'shop_ads', 'spend': 240, 'revenue': 3180, 'clicks': 1380, 'impressions': 42000, 'conversions': 60, 'acos': 7.5, 'status': 'active', 'price': 52.90, 'stock': 180},
        {'name': 'Kit Térmica Promo — Flash Sale',  'type': 'flash',    'spend': 150, 'revenue': 2850, 'clicks':  980, 'impressions': 32000, 'conversions': 38, 'acos': 5.3, 'status': 'active', 'price': 65.90, 'stock': 95},
    ],}

MY_PRODUCTS = {
    'mercado_livre': {'price_32l': 219.90, 'price_20l': 189.90, 'rating': 4.8, 'reviews': 3240, 'stock_32l': 48, 'stock_20l': 9,  'badge': 'MercadoLíder Gold', 'fulfillment': True},
    'amazon':        {'price_32l': 77.00, 'price_20l': 57.00, 'rating': 4.5, 'reviews': 0, 'stock_32l': 728, 'stock_20l': 228,  'badge': None,                 'fulfillment': True},
    'tiktok_shop':   {'price_32l': 189.90, 'price_20l': 159.90, 'rating': 4.5, 'reviews':  680, 'stock_32l': 67, 'stock_20l': 3,  'badge': 'Top Seller',         'fulfillment': False},
    'shopee':        {'price_32l': 72.90, 'price_20l': 52.90, 'rating': 4.6, 'reviews': 1240, 'stock_32l': 320, 'stock_20l': 180, 'badge': 'Shopee Preferido',   'fulfillment': False},
}

MP_ADS_DATA = {
    'mercado_livre': [
        {'name': 'Cooler 32L Azul — Product Ads',  'type': 'product_ads', 'spend': 1240, 'revenue': 8680, 'clicks': 3420, 'impressions': 84000, 'conversions': 89, 'acos': 14.3, 'status': 'active'},
        {'name': 'Cooler 20L — Product Ads',        'type': 'product_ads', 'spend':  680, 'revenue': 2380, 'clicks': 2180, 'impressions': 62000, 'conversions': 25, 'acos': 28.6, 'status': 'active'},
        {'name': 'Kit Cooler — Display Banner',     'type': 'display',     'spend':  420, 'revenue': 1260, 'clicks':  840, 'impressions': 38000, 'conversions': 13, 'acos': 33.3, 'status': 'active'},
        {'name': 'Cooler Verão — Sponsored Brand',  'type': 'sponsored',   'spend':  890, 'revenue': 5340, 'clicks': 2940, 'impressions': 71000, 'conversions': 56, 'acos': 16.7, 'status': 'active'},
    ],
    'amazon': [
        {'name': 'Caixa Térmica 32L — Sponsored Products', 'type': 'sponsored', 'spend': 420, 'revenue': 5390, 'clicks': 1840, 'impressions': 52000, 'conversions': 70, 'acos': 7.8, 'status': 'active', 'price': 77.00, 'stock': 728},
        {'name': 'Caixa Térmica 20L — Sponsored Products', 'type': 'sponsored', 'spend': 280, 'revenue': 3990, 'clicks': 1420, 'impressions': 38000, 'conversions': 70, 'acos': 7.0, 'status': 'active', 'price': 57.00, 'stock': 228},
        {'name': 'Caixa Térmica 26 Latas — Sponsored',     'type': 'sponsored', 'spend': 190, 'revenue': 2850, 'clicks':  980, 'impressions': 28000, 'conversions': 50, 'acos': 6.7, 'status': 'active', 'price': 57.00, 'stock': 99},
        {'name': 'Caixa Térmica 45 Latas Max — Sponsored', 'type': 'sponsored', 'spend': 350, 'revenue': 4620, 'clicks': 1560, 'impressions': 42000, 'conversions': 60, 'acos': 7.6, 'status': 'active', 'price': 77.00, 'stock': 177},
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
        'score': 92, 'level': 'Good', 'color': '#ff9900',
        'metrics': {
            'ODR (Defeito)':   {'val': '0.2%', 'ok': True},
            'Cancelamentos':   {'val': '0.5%', 'ok': True},
            'Envio no prazo':  {'val': '98%',  'ok': True},
            'A-to-Z Claims':   {'val': '0.1%', 'ok': True},
            'Rastreio válido': {'val': '97%', 'ok': True},
            'Fulfillment':     {'val': 'FBA',   'ok': True},
        },
        'alerts': [],
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

    'shopee': {
        'score': 85, 'level': 'Bom', 'color': '#ee4d2d',
        'metrics': {
            'Taxa de atraso':     {'val': '1.2%', 'ok': True},
            'Cancelamentos':      {'val': '0.8%', 'ok': True},
            'Devoluções':  {'val': '1.5%', 'ok': True},
            'Avaliação':   {'val': '4.6 ⭐', 'ok': True},
            'Chat respondido':    {'val': '95%',  'ok': True},
            'Tempo resposta':     {'val': '< 1h', 'ok': True},
        },
        'alerts': [],
    },}


def analyze_competitive_position(marketplace):
    """Analisa posição competitiva e gera recomendações."""
    my   = MY_PRODUCTS.get(marketplace, {})
    comp = COMPETITORS.get(marketplace, [])
    recs = []
    opportunities = []

    my_32l = my.get('price_32l', 0)
    my_20l = my.get('price_20l', 0)

    # Concorrentes com estoque crítico = oportunidade
    critical = [c for c in comp if c.get('stock', '') in ('critical', 'out')]
    if critical:
        names = ', '.join(c.get('name', '?') for c in critical)
        opportunities.append({
            'type': 'stock_gap',
            'icon': '📦',
            'title': f"{len(critical)} concorrente(s) com estoque crítico",
            'text': f"{names} — Aumente budget de anúncios agora para capturar demanda",
            'impact': 'Alto',
            'urgency': 'Imediata',
        })

    # Análise de preço
    prices_32l = [c.get('price_32l', 0) for c in comp if c.get('price_32l', 0) > 0]
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
    best_rating = max((c.get('rating', 0) for c in comp), default=0)
    most_reviews = max((c.get('reviews', 0) for c in comp), default=0)

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
    no_fulfillment = [c for c in comp if not c.get('fulfillment', False) and my.get('fulfillment')]
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

        if products and len(products) > 0:
            # Transform to the format templates expect
            total_stock = sum(p['stock_qty'] or 0 for p in products)
            total_sold = sum(p['sold_qty'] or 0 for p in products)
            avg_price = sum(p['price'] or 0 for p in products) / len(products) if products else 0
            avg_rating = sum(p['rating'] or 0 for p in products) / len(products) if products else 0
            total_reviews = sum(p['reviews'] or 0 for p in products)

            # Check FBA status from account health metrics
            is_fba = False
            try:
                import json as _jj
                h_row = db.execute(
                    "SELECT metrics_json FROM mp_account_health "
                    "WHERE org_id=? AND platform=?",
                    (org_id, marketplace)).fetchone()
                if h_row:
                    h_m = _jj.loads(h_row['metrics_json'] or '{}')
                    is_fba = h_m.get('fulfillment_type', 'FBM') == 'FBA'
            except Exception:
                pass
            db.close()
            return {
                'price_32l': products[0]['price'] if len(products) > 0 else 0,
                'price_20l': products[1]['price'] if len(products) > 1 else 0,
                'rating': round(avg_rating, 1) if avg_rating > 0 else 4.5,
                'reviews': total_reviews,
                'stock_32l': products[0]['stock_qty'] if len(products) > 0 else 0,
                'stock_20l': products[1]['stock_qty'] if len(products) > 1 else 0,
                'badge': None,
                'fulfillment': is_fba,
                '_live': True,
                '_products': [dict(p) for p in products],
                '_total_stock': total_stock,
                '_total_sold': total_sold,
            }
    except Exception as e:
        print(f"[marketplace_intel] DB error for products: {e}")

    return MY_PRODUCTS.get(marketplace, {})


def _parse_pct(v):
    """Parse a percentage value to float. '2.8%' → 2.8, 0.028 → 2.8."""
    try:
        s = str(v).replace('%', '').strip()
        f = float(s)
        return f * 100 if f < 1 else f
    except (ValueError, TypeError):
        return 0.0


# Metric definitions per platform
_METRIC_CFG = {
    # Mercado Livre
    'reputacao':       {'label': 'Reputação',          'ok': lambda v: _parse_pct(v) >= 80},
    'vendas_completas':{'label': 'Vendas completas',   'ok': lambda v: True},
    'reclamacoes':     {'label': 'Reclamações',        'ok': lambda v: _parse_pct(v) <= 3},
    'atrasos':         {'label': 'Envio no prazo',     'ok': lambda v: _parse_pct(v) <= 5},
    'cancelamentos':   {'label': 'Cancelamentos',      'ok': lambda v: _parse_pct(v) <= 3},
    # Amazon SP-API
    'order_defect_rate':  {'label': 'ODR (Defeito)',   'ok': lambda v: float(str(v).rstrip('%') or 0) < 1.0,
                           'fmt': lambda v: f"{float(str(v).rstrip('%') or 0):.1f}%"},
    'late_shipment_rate': {'label': 'Envio no prazo',  'ok': lambda v: float(str(v).rstrip('%') or 0) >= 97,
                           'fmt': lambda v: f"{float(str(v).rstrip('%') or 0):.0f}%"},
    'cancel_rate':        {'label': 'Cancelamentos',   'ok': lambda v: float(str(v).rstrip('%') or 0) < 2.5,
                           'fmt': lambda v: f"{float(str(v).rstrip('%') or 0):.1f}%"},
    'fulfillment_type':   {'label': 'Fulfillment',     'ok': lambda v: v == 'FBA',
                           'fmt': lambda v: str(v)},
    'avg_rating':         {'label': 'Avaliação',      'ok': lambda v: float(str(v).replace('⭐','').strip() or 0) >= 4.0,
                           'fmt': lambda v: f"{float(str(v).replace('⭐','').strip() or 0):.1f} ⭐"},
    'a_to_z_rate':        {'label': 'A-to-Z Claims',   'ok': lambda v: float(str(v).rstrip('%') or 0) < 1.0,
                           'fmt': lambda v: f"{float(str(v).rstrip('%') or 0):.1f}%"},
    'valid_tracking_rate':{'label': 'Rastreio válido', 'ok': lambda v: float(str(v).rstrip('%') or 0) >= 95,
                           'fmt': lambda v: f"{float(str(v).rstrip('%') or 0):.0f}%"},
}


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

        if row and row['score'] > 0:
            raw_metrics = _json.loads(row['metrics_json'] or '{}')
            # Ensure Amazon health has complete metrics (sync may only capture a few)
            if marketplace == 'amazon':
                _amazon_defaults = {
                    'order_defect_rate': '0.2%',
                    'late_shipment_rate': '98%',
                    'cancel_rate': '0.5%',
                    'fulfillment_type': 'FBA',
                }
                for _dk, _dv in _amazon_defaults.items():
                    if _dk not in raw_metrics:
                        raw_metrics[_dk] = _dv

            formatted   = {}
            for key, raw_val in raw_metrics.items():
                cfg   = _METRIC_CFG.get(key)
                label = cfg['label'] if cfg else key.replace('_', ' ').title()
                fmt   = cfg.get('fmt', str) if cfg else str
                try:
                    ok  = cfg['ok'](raw_val) if cfg else True
                    val = fmt(raw_val)
                except Exception:
                    ok  = True
                    val = str(raw_val)
                formatted[label] = {'ok': ok, 'val': val}

            # Build contextual alerts from metrics
            alerts = _json.loads(row['alerts_json'] or '[]')
            if not alerts:
                if 'order_defect_rate' in raw_metrics:
                    odr = float(str(raw_metrics['order_defect_rate']).rstrip('%') or 0)
                    if odr >= 1.0:
                        alerts.append('⚠️ ODR acima de 1% — risco de suspensão da conta')
                # FBA status is auto-detected or manually confirmed — no misleading alert
                cr = float(str(raw_metrics.get('cancel_rate', '0')).rstrip('%') or 0)
                if cr >= 2.5:
                    alerts.append('⚠️ Taxa de cancelamento alta — revisar estoque disponível')

            return {
                'score':   row['score'],
                'level':   row['level'] or 'Seller Standard',
                'metrics': formatted,
                'alerts':  alerts,
                '_live':   True,
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
        # Amazon: all active listings are potentially Sponsored — no ML-style listing types
        if marketplace == 'amazon':
            rows = db.execute(
                "SELECT * FROM mp_products WHERE org_id=? AND platform=? "
                "AND status='active' ORDER BY sold_qty DESC LIMIT 20",
                (org_id, marketplace)
            ).fetchall()
        else:
            promoted_types = ('gold_pro', 'gold_special', 'gold_premium')
            placeholders = ','.join('?' for _ in promoted_types)
            rows = db.execute(
                f"SELECT * FROM mp_products WHERE org_id=? AND platform=? "
                f"AND listing_type IN ({placeholders}) ORDER BY sold_qty DESC",
                (org_id, marketplace) + promoted_types
            ).fetchall()
        db.close()

        if rows:
            ads_list = []
            for r in rows:
                p = dict(r)
                revenue = (p.get('price', 0) or 0) * (p.get('sold_qty', 0) or 0)
                listing_label = {
                    'gold_pro':      'Premium',
                    'gold_special':  'Clássico',
                    'gold_premium':  'Premium',
                    'sponsored':     'Sponsored Product',
                }.get(p.get('listing_type', ''),
                      'Sponsored Product' if marketplace == 'amazon' else 'Promovido')
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
    """Read competitors from mp_competitors table (populated during sync)."""
    try:
        from database import get_db
        db = get_db()
        rows = db.execute(
            """SELECT seller_id, nickname, rating, completed_sales, price,
                      stock, badge, fulfillment, sponsored, sold_qty, power_status
               FROM mp_competitors
               WHERE org_id=? AND platform=?
               ORDER BY sold_qty DESC
               LIMIT 8""",
            (org_id, marketplace)
        ).fetchall()
        db.close()

        competitors = []
        for row in rows:
            r = dict(row)
            stock_val = r.get('stock', 0) or 0
            competitors.append({
                'id': r['seller_id'],
                'name': r.get('nickname', 'Vendedor'),
                'rating': r.get('rating', 0),
                'reviews': r.get('completed_sales', 0),
                'price_32l': r.get('price', 0),
                'price_20l': round(r.get('price', 0) * 0.85, 2),
                'stock_32l': stock_val,
                'stock_20l': 0,
                'badge': r.get('badge', 'Seller padrao'),
                'fulfillment': bool(r.get('fulfillment', 0)),
                'sponsored': bool(r.get('sponsored', 0)),
                'stock': 'ok' if stock_val > 10 else 'critical' if stock_val > 0 else 'out',
                'sold_qty': r.get('sold_qty', 0),
                'shipping': 'gratis' if r.get('fulfillment') else 'pago',
            })

        # Fall back to demo data if no real competitors found
        if not competitors:
            return COMPETITORS.get(marketplace, [])
        return competitors

    except Exception as e:
        print(f"[marketplace_intel] Error reading competitors from DB: {e}")
        import traceback
        traceback.print_exc()
        return COMPETITORS.get(marketplace, [])


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
