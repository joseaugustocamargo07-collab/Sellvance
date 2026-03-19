"""
Sellvance — Motor de Estratégia de Marketplaces
Calcula scores de alocação, posição competitiva e recomendações de re-bid por marketplace.
"""

# ── Estrutura de comissões ML por tipo de anúncio ─────────────────────────────
ML_LISTING_FEES = {
    'gold_premium': 0.09,   # Ouro Premium — máxima visibilidade, menor taxa
    'gold_pro':     0.13,   # Ouro Pro — alta visibilidade
    'gold_special': 0.16,   # Ouro Especial
    'gold':         0.16,   # Ouro
    'silver':       0.10,   # Prata
    'bronze':       0.06,   # Bronze
    'free':         0.05,   # Grátis
}

ML_LISTING_LABELS = {
    'gold_premium': 'Ouro Premium',
    'gold_pro':     'Ouro Pro',
    'gold_special': 'Ouro Especial',
    'gold':         'Ouro',
    'silver':       'Prata',
    'bronze':       'Bronze',
    'free':         'Grátis',
}

PLATFORM_META = {
    'mercado_livre': {'name': 'Mercado Livre', 'icon': '🛒', 'color': '#ffe600', 'fee_base': 0.16, 'fulfillment': True},
    'amazon':        {'name': 'Amazon',         'icon': '📦', 'color': '#ff9900', 'fee_base': 0.15, 'fulfillment': True},
    'tiktok_shop':   {'name': 'TikTok Shop',    'icon': '🎵', 'color': '#ff0050', 'fee_base': 0.05, 'fulfillment': False},
    'shopee':        {'name': 'Shopee',          'icon': '🛍️', 'color': '#f05d23', 'fee_base': 0.14, 'fulfillment': False},
}


def compute_marketplace_scores(org_id):
    """
    Calcula score estratégico (0-100) por marketplace conectado.
    Retorna lista ordenada por score decrescente.
    """
    from database import get_db

    results = []

    # Only score platforms that have competitor data (i.e., are synced)
    try:
        db = get_db()
        active_platforms = [
            r[0] for r in db.execute(
                "SELECT DISTINCT platform FROM mp_competitors WHERE org_id=?", (org_id,)
            ).fetchall()
        ]
        db.close()
    except Exception:
        active_platforms = []

    # Always include mercado_livre if we have products
    try:
        db = get_db()
        if db.execute(
            "SELECT COUNT(*) FROM mp_products WHERE org_id=? AND platform='mercado_livre' AND status='active'",
            (org_id,)
        ).fetchone()[0] > 0:
            if 'mercado_livre' not in active_platforms:
                active_platforms.insert(0, 'mercado_livre')
        db.close()
    except Exception:
        pass

    for platform in active_platforms:
        try:
            db = get_db()

            # ── Competitor stats ───────────────────────────────────────────
            comp = db.execute("""
                SELECT
                    COUNT(*) as total,
                    COALESCE(AVG(price), 0) as avg_price,
                    COALESCE(MIN(price), 0) as min_price,
                    COALESCE(MAX(price), 0) as max_price,
                    COUNT(CASE WHEN badge LIKE '%Platinum%' THEN 1 END) as platinum_count,
                    COUNT(CASE WHEN badge LIKE '%Gold%' OR badge LIKE '%Lider%' OR badge LIKE '%Lider%' THEN 1 END) as gold_count,
                    COUNT(CASE WHEN fulfillment=1 THEN 1 END) as full_count,
                    COALESCE(SUM(sold_qty), 0) as total_market_sales,
                    COALESCE(AVG(rating), 0) as avg_rating
                FROM mp_competitors
                WHERE org_id=? AND platform=?
            """, (org_id, platform)).fetchone()

            # ── Our product stats ──────────────────────────────────────────
            my = db.execute("""
                SELECT
                    COUNT(*) as total_products,
                    COALESCE(AVG(price), 0) as avg_price,
                    COALESCE(SUM(sold_qty), 0) as our_sales,
                    COALESCE(AVG(rating), 0) as our_rating
                FROM mp_products
                WHERE org_id=? AND platform=? AND status='active'
            """, (org_id, platform)).fetchone()

            db.close()

            comp = dict(comp) if comp else {}
            my   = dict(my)   if my   else {}

            num_comp         = comp.get('total', 0) or 0
            avg_comp_price   = comp.get('avg_price', 0) or 0
            min_comp_price   = comp.get('min_price', 0) or 0
            max_comp_price   = comp.get('max_price', 0) or 0
            platinum_count   = comp.get('platinum_count', 0) or 0
            gold_count       = comp.get('gold_count', 0) or 0
            full_sellers     = comp.get('full_count', 0) or 0
            market_sales     = comp.get('total_market_sales', 0) or 0
            avg_comp_rating  = comp.get('avg_rating', 0) or 0

            our_price        = my.get('avg_price', 0) or 0
            our_sales        = my.get('our_sales', 0) or 0
            our_rating       = my.get('our_rating', 0) or 0
            total_products   = my.get('total_products', 0) or 0

            # ── Competition Score (alto = menor concorrência = melhor oportunidade) ──
            if num_comp == 0:
                competition_score = 78
            else:
                base = max(0, 100 - (num_comp * 6))
                power_penalty   = (platinum_count * 18) + (gold_count * 8)
                fulfill_penalty = full_sellers * 4
                competition_score = max(8, min(100, base - power_penalty - fulfill_penalty))

            # ── Price Position Score ───────────────────────────────────────
            if our_price > 0 and avg_comp_price > 0:
                ratio = our_price / avg_comp_price
                if ratio <= 0.90:
                    price_score = 92   # Abaixo da média → vantagem forte
                elif ratio <= 1.05:
                    price_score = 78   # Na média → saudável
                elif ratio <= 1.15:
                    price_score = 52   # Levemente acima → risco
                else:
                    price_score = 28   # Muito acima → alto risco
            else:
                price_score = 62

            # ── Demand Score (baseado em atividade do mercado) ─────────────
            if market_sales > 500:
                demand_score = 90
            elif market_sales > 100:
                demand_score = 75
            elif market_sales > 0:
                demand_score = 60
            elif num_comp > 0:
                demand_score = 52   # Tem concorrentes = tem demanda
            else:
                demand_score = 42

            # ── Margin Score (estimativa após comissão do marketplace) ─────
            fee_base = PLATFORM_META.get(platform, {}).get('fee_base', 0.16)
            if our_price > 0:
                margin_score = int((1 - fee_base) * 100)  # ~84 para ML
            else:
                margin_score = 72

            # ── Overall Score (pesos calibrados por impacto em conversão) ──
            overall = int(
                competition_score * 0.38 +
                price_score       * 0.30 +
                demand_score      * 0.20 +
                margin_score      * 0.12
            )

            # ── Recomendação ───────────────────────────────────────────────
            if overall >= 68:
                recommendation = 'Priorizar'
                rec_color = '#10b981'
                rec_icon  = '🟢'
            elif overall >= 48:
                recommendation = 'Manter'
                rec_color = '#f59e0b'
                rec_icon  = '🟡'
            else:
                recommendation = 'Revisar Estratégia'
                rec_color = '#ef4444'
                rec_icon  = '🔴'

            # ── Motivo resumido ────────────────────────────────────────────
            reasons = []
            if competition_score >= 70:
                reasons.append('Baixa concorrência')
            elif competition_score < 40:
                reasons.append('Alta concorrência')
            if price_score >= 78:
                reasons.append('Preço competitivo')
            elif price_score < 50:
                reasons.append('Preço acima da média')
            if demand_score >= 72:
                reasons.append('Alta demanda')
            if not reasons:
                reasons.append('Mercado equilibrado')
            reason = ' · '.join(reasons[:2])

            meta = PLATFORM_META.get(platform, {'name': platform, 'icon': '🛍️', 'color': '#6b7280'})
            results.append({
                'platform':          platform,
                'name':              meta['name'],
                'icon':              meta['icon'],
                'color':             meta['color'],
                'score':             overall,
                'competition_score': competition_score,
                'price_score':       price_score,
                'demand_score':      demand_score,
                'margin_score':      margin_score,
                'recommendation':    recommendation,
                'rec_color':         rec_color,
                'rec_icon':          rec_icon,
                'reason':            reason,
                'num_competitors':   num_comp,
                'avg_comp_price':    round(avg_comp_price, 2),
                'min_comp_price':    round(min_comp_price, 2),
                'max_comp_price':    round(max_comp_price, 2),
                'our_price':         round(our_price, 2),
                'our_sales':         our_sales,
                'market_sales':      market_sales,
                'total_products':    total_products,
                'platinum_count':    platinum_count,
                'gold_count':        gold_count,
                'full_sellers':      full_sellers,
            })

        except Exception as e:
            meta = PLATFORM_META.get(platform, {'name': platform, 'icon': '🛍️', 'color': '#6b7280'})
            results.append({
                'platform': platform, 'name': meta['name'], 'icon': meta['icon'],
                'color': '#6b7280', 'score': 0, 'error': str(e),
                'recommendation': 'Sem dados', 'rec_color': '#6b7280', 'rec_icon': '⚪',
                'reason': 'Aguardando sincronização',
                'competition_score': 0, 'price_score': 0, 'demand_score': 0, 'margin_score': 0,
                'num_competitors': 0, 'avg_comp_price': 0, 'min_comp_price': 0,
                'max_comp_price': 0, 'our_price': 0, 'our_sales': 0, 'market_sales': 0,
                'total_products': 0, 'platinum_count': 0, 'gold_count': 0, 'full_sellers': 0,
            })

    results.sort(key=lambda x: x['score'], reverse=True)
    return results


def get_rebid_recommendations(org_id, platform='mercado_livre'):
    """
    Recomendações de re-bid por produto.
    Retorna: tipo atual → tipo recomendado, motivo, ganho estimado mensal.
    """
    from database import get_db

    try:
        db = get_db()

        products = db.execute("""
            SELECT external_id, title, price, stock_qty, sold_qty,
                   COALESCE(listing_type, 'gold_pro') as listing_type
            FROM mp_products
            WHERE org_id=? AND platform=? AND status='active'
            ORDER BY COALESCE(sold_qty, 0) DESC
            LIMIT 12
        """, (org_id, platform)).fetchall()

        comp_summary = db.execute("""
            SELECT
                COALESCE(AVG(price), 0) as avg_price,
                COUNT(*) as total,
                COUNT(CASE WHEN badge LIKE '%Platinum%' THEN 1 END) as platinum,
                COUNT(CASE WHEN badge LIKE '%Gold%' OR badge LIKE '%Lider%' THEN 1 END) as gold,
                COUNT(CASE WHEN fulfillment=1 THEN 1 END) as full_sellers
            FROM mp_competitors
            WHERE org_id=? AND platform=?
        """, (org_id, platform)).fetchone()

        db.close()

        comp = dict(comp_summary) if comp_summary else {}
        avg_comp_price = comp.get('avg_price', 0) or 0
        platinum_count = comp.get('platinum', 0) or 0
        gold_count     = comp.get('gold', 0) or 0
        full_sellers   = comp.get('full_sellers', 0) or 0
        num_comp       = comp.get('total', 0) or 0

        recommendations = []
        for row in products:
            p            = dict(row)
            current_type = (p.get('listing_type') or 'gold_pro').lower().replace(' ', '_')
            price        = p.get('price', 0) or 0
            sold_qty     = p.get('sold_qty', 0) or 0
            stock_qty    = p.get('stock_qty', 0) or 0
            title        = (p.get('title') or 'Produto')[:45]

            # Decide recommendation
            rec_type    = current_type
            rec_reason  = 'Tipo de anúncio adequado ao momento'
            monthly_gain = 0
            urgency      = 'normal'  # normal | high | low

            has_platinum_rivals = platinum_count >= 2
            price_above_avg     = avg_comp_price > 0 and price > avg_comp_price * 1.10

            if has_platinum_rivals and current_type not in ('gold_premium',):
                # High-power rivals → go premium to stay visible
                rec_type     = 'gold_premium'
                rec_reason   = f'{platinum_count} concorrentes Platinum detectados — visibilidade máxima recomendada'
                monthly_gain = int(sold_qty * price * 0.12) if sold_qty > 0 else int(price * 8)
                urgency      = 'high'

            elif price_above_avg:
                # Price issue — can't fix with listing type, flag for repricing
                rec_type    = current_type
                rec_reason  = f'Preço {round((price/avg_comp_price-1)*100, 0):.0f}% acima da média (R${avg_comp_price:.2f}) — revisar precificação'
                monthly_gain = int((price - avg_comp_price) * max(sold_qty, 1) * 0.25)
                urgency      = 'high'

            elif sold_qty > 80 and current_type in ('free', 'bronze', 'silver'):
                # High volume on low-visibility listing → upgrade
                rec_type    = 'gold_pro'
                rec_reason  = f'Alto giro ({sold_qty} vendas) — upgrade aumenta visibilidade e conversão'
                monthly_gain = int(sold_qty * price * 0.10)
                urgency     = 'high'

            elif sold_qty > 150 and current_type in ('gold', 'gold_special', 'gold_pro'):
                # Very high volume → gold_premium to dominate
                rec_type    = 'gold_premium'
                rec_reason  = f'Volume muito alto ({sold_qty} vendas) — Ouro Premium maximiza Buy Box'
                monthly_gain = int(sold_qty * price * 0.06)
                urgency     = 'high'

            elif full_sellers > 3 and current_type not in ('gold_premium', 'gold_pro'):
                # Many fulfillment competitors → need competitive listing
                rec_type    = 'gold_pro'
                rec_reason  = f'{full_sellers} concorrentes com fulfillment — anúncio Ouro recomendado'
                monthly_gain = int(price * 15)
                urgency     = 'normal'

            elif stock_qty < 5 and stock_qty > 0:
                rec_type    = current_type
                rec_reason  = f'Estoque crítico ({stock_qty} un.) — reabastecer antes de investir em bid'
                monthly_gain = 0
                urgency     = 'high'

            current_label = ML_LISTING_LABELS.get(current_type, current_type.replace('_', ' ').title())
            rec_label     = ML_LISTING_LABELS.get(rec_type, rec_type.replace('_', ' ').title())
            is_change     = rec_type != current_type

            recommendations.append({
                'title':        title,
                'current_type': current_label,
                'rec_type':     rec_label,
                'reason':       rec_reason,
                'monthly_gain': monthly_gain,
                'is_change':    is_change,
                'price':        round(price, 2),
                'sold_qty':     sold_qty,
                'stock_qty':    stock_qty,
                'urgency':      urgency,
            })

        return recommendations

    except Exception as e:
        return [{'error': str(e), 'title': 'Erro', 'current_type': '—', 'rec_type': '—',
                 'reason': str(e), 'monthly_gain': 0, 'is_change': False,
                 'price': 0, 'sold_qty': 0, 'stock_qty': 0, 'urgency': 'normal'}]
