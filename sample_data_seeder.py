# sample_data_seeder.py — Gera dados simulados realistas para demo
# Popula a org com:
#   - 42 produtos (stock_items)
#   - 120 clientes (contacts)
#   - 380 pedidos (orders) espalhados nos ultimos 90 dias
#   - 8 campanhas de anuncios (ad_campaigns)
#   - 60 precos de competidores (competitor_prices)
#   - 15 snapshots de Buy Box
#   - 3 alertas de fraude pendentes
#
# Tudo deterministico com seed fixa para producao consistente.

import random
from datetime import datetime, timedelta
from database import get_db


SAMPLE_PRODUCTS = [
    # (sku, name, category, brand, cost, sale, marketplace)
    ('FONE-JBL-T110', 'Fone de Ouvido JBL Tune 110', 'eletronicos', 'JBL', 28.00, 69.90, 'mercado_livre'),
    ('SMARTWATCH-MI-BAND8', 'Smartwatch Xiaomi Mi Band 8', 'eletronicos', 'Xiaomi', 110.00, 229.00, 'mercado_livre'),
    ('CARREGADOR-TURBO-20W', 'Carregador Turbo 20W USB-C', 'eletronicos', 'Geonav', 14.00, 39.90, 'mercado_livre'),
    ('CABO-USB-C-2M', 'Cabo USB-C 2 Metros Reforcado', 'eletronicos', 'Geonav', 8.50, 29.90, 'mercado_livre'),
    ('CAPINHA-IPHONE-15', 'Capinha Transparente iPhone 15', 'acessorios', 'Generico', 9.00, 34.90, 'mercado_livre'),
    ('WEBCAM-HD-1080P', 'Webcam Full HD 1080p USB', 'eletronicos', 'Multilaser', 45.00, 119.00, 'mercado_livre'),
    ('TECLADO-MECANICO-RGB', 'Teclado Mecanico Gamer RGB', 'gamer', 'Redragon', 105.00, 259.00, 'mercado_livre'),
    ('MOUSE-GAMER-RGB', 'Mouse Gamer 6400 DPI RGB', 'gamer', 'Redragon', 40.00, 109.00, 'mercado_livre'),
    ('SUPORTE-NOTEBOOK', 'Suporte Ajustavel para Notebook', 'acessorios', 'Generico', 18.00, 59.90, 'mercado_livre'),
    ('HUB-USB-7PORTAS', 'Hub USB 3.0 com 7 Portas', 'eletronicos', 'Generico', 28.00, 89.00, 'mercado_livre'),
    ('BATOM-RUBY-ROSE-MATTE', 'Batom Matte Ruby Rose', 'beleza', 'Ruby Rose', 5.50, 22.90, 'mercado_livre'),
    ('PERFUME-IMPORTADO-100ML', 'Perfume Importado Masculino 100ml', 'beleza', 'Premium', 140.00, 329.00, 'mercado_livre'),
    ('SHAMPOO-ANTIQUEDA-300ML', 'Shampoo Antiqueda 300ml', 'beleza', 'Kerastase', 18.00, 54.90, 'mercado_livre'),
    ('HIDRATANTE-FACIAL-50G', 'Hidratante Facial com Acido Hialuronico', 'beleza', 'Vichy', 15.00, 49.90, 'mercado_livre'),
    ('AIR-FRYER-5L', 'Air Fryer Digital 5 Litros', 'casa', 'Mondial', 180.00, 399.00, 'mercado_livre'),
    ('CAFETEIRA-ELETRICA-15X', 'Cafeteira Eletrica 15 Xicaras', 'casa', 'Britania', 82.00, 199.00, 'mercado_livre'),
    ('LIQUIDIFICADOR-1200W', 'Liquidificador 1200W 4 Velocidades', 'casa', 'Oster', 65.00, 179.00, 'mercado_livre'),
    ('PANELA-ANTIADERENTE-26CM', 'Panela Antiaderente 26cm', 'casa', 'Tramontina', 35.00, 109.00, 'mercado_livre'),
    ('JOGO-PANELAS-5PCS', 'Jogo de Panelas Antiaderente 5 pecas', 'casa', 'Tramontina', 140.00, 349.00, 'mercado_livre'),
    ('HALTER-AJUSTAVEL-5KG', 'Halter Ajustavel 5kg (par)', 'fitness', 'Kikos', 42.00, 129.00, 'mercado_livre'),
    ('CORDA-PULAR-PROFISSIONAL', 'Corda de Pular Rolamento Profissional', 'fitness', 'Acte', 10.00, 42.90, 'mercado_livre'),
    ('FAIXA-ELASTICA-KIT-5', 'Kit 5 Faixas Elasticas Resistencia', 'fitness', 'Acte', 15.00, 59.90, 'mercado_livre'),
    ('GARRAFA-TERMICA-1L', 'Garrafa Termica Inox 1 Litro', 'fitness', 'Stanley', 18.00, 64.90, 'mercado_livre'),
    ('CAMISETA-BASICA-ALGODAO', 'Camiseta Basica 100% Algodao', 'moda', 'Hering', 11.00, 44.90, 'mercado_livre'),
    ('TENIS-RUNNING-MASCULINO', 'Tenis Running Masculino', 'moda', 'Nike', 95.00, 259.00, 'mercado_livre'),
    ('BONE-ABA-CURVA', 'Bone Aba Curva Snapback', 'moda', 'New Era', 12.00, 49.90, 'mercado_livre'),
    ('MEIA-ESPORTIVA-KIT-6', 'Kit 6 Meias Esportivas Cano Medio', 'moda', 'Lupo', 18.00, 69.90, 'mercado_livre'),
    ('MOCHILA-ESCOLAR-RESISTENTE', 'Mochila Escolar Resistente 25L', 'escolar', 'Sestini', 58.00, 159.00, 'mercado_livre'),
    ('BRINQUEDO-EDUCATIVO-MONTESSORI', 'Brinquedo Educativo Montessori', 'infantil', 'Estrela', 32.00, 109.00, 'mercado_livre'),
    # Amazon exclusivos
    ('KINDLE-PAPERWHITE', 'Kindle Paperwhite 11a Geracao', 'eletronicos', 'Amazon', 380.00, 599.00, 'amazon'),
    ('ECHO-DOT-5', 'Echo Dot 5a Geracao', 'eletronicos', 'Amazon', 220.00, 389.00, 'amazon'),
    ('FIRE-TV-STICK-4K', 'Fire TV Stick 4K', 'eletronicos', 'Amazon', 180.00, 329.00, 'amazon'),
    # Shopee aggressive
    ('CAIXA-SOM-BLUETOOTH', 'Caixa de Som Bluetooth Portatil', 'eletronicos', 'Generico', 35.00, 89.90, 'shopee'),
    ('ANEL-LED-RING-LIGHT', 'Ring Light 10 polegadas com Tripe', 'acessorios', 'Generico', 45.00, 119.00, 'shopee'),
    ('BOLSA-TERMICA-MARMITA', 'Bolsa Termica para Marmita 5L', 'casa', 'Generico', 22.00, 64.90, 'shopee'),
    ('RELOGIO-DIGITAL-ESPORTIVO', 'Relogio Digital Esportivo a Prova Dagua', 'moda', 'Skmei', 38.00, 99.00, 'shopee'),
    # TikTok Shop trending
    ('CILIOS-MAGNETICOS', 'Cilios Magneticos com Delineador', 'beleza', 'TiTi', 25.00, 69.90, 'tiktok_shop'),
    ('ILUMINADOR-FACIAL-STICK', 'Iluminador Facial em Bastao', 'beleza', 'Ruby Rose', 12.00, 39.90, 'tiktok_shop'),
    ('BONECA-REBORN-REALISTA', 'Boneca Reborn Realista 40cm', 'infantil', 'Laura Doll', 180.00, 449.00, 'tiktok_shop'),
    ('ESTICADOR-CABELO-PROFI', 'Esticador de Cabelo Profissional', 'beleza', 'Taiff', 155.00, 329.00, 'tiktok_shop'),
    ('CANETA-GEL-KIT-24', 'Kit 24 Canetas Gel Coloridas', 'escolar', 'BIC', 22.00, 64.90, 'shopee'),
    ('CADERNO-INTELIGENTE-A5', 'Caderno Inteligente A5 Reorganizavel', 'escolar', 'Caderno Inteligente', 48.00, 129.00, 'mercado_livre'),
]

SAMPLE_NAMES = [
    'Ana Silva', 'Carlos Souza', 'Maria Oliveira', 'Joao Santos', 'Paula Costa',
    'Rafael Lima', 'Juliana Alves', 'Bruno Pereira', 'Fernanda Ribeiro', 'Lucas Martins',
    'Camila Rodrigues', 'Pedro Carvalho', 'Beatriz Gomes', 'Gustavo Dias', 'Larissa Araujo',
    'Felipe Barbosa', 'Isabela Cardoso', 'Thiago Mendes', 'Leticia Rocha', 'Rodrigo Nunes',
    'Amanda Freitas', 'Diego Castro', 'Gabriela Moura', 'Marcelo Teixeira', 'Patricia Lopes',
    'Ricardo Campos', 'Natalia Correia', 'Eduardo Andrade', 'Carolina Vieira', 'Leonardo Ramos',
]

CHANNELS = ['mercado_livre', 'amazon', 'shopee', 'tiktok_shop']
CHANNEL_WEIGHTS = [0.50, 0.20, 0.20, 0.10]

RETURN_REASONS = ['mudei_ideia', 'nao_gostei', 'tamanho_errado', 'defeito', 'outro']


def _rand_date(days_ago_min, days_ago_max):
    delta = random.randint(days_ago_min, days_ago_max)
    d = datetime.now() - timedelta(days=delta, hours=random.randint(0, 23),
                                    minutes=random.randint(0, 59))
    return d.strftime('%Y-%m-%d %H:%M:%S')


def seed_products(db, org_id):
    """Popula stock_items."""
    count = 0
    for p in SAMPLE_PRODUCTS:
        sku, name, category, brand, cost, sale, mp = p
        existing = db.execute(
            'SELECT id FROM stock_items WHERE org_id=? AND sku=?',
            (org_id, sku)
        ).fetchone()
        if existing:
            continue
        db.execute(
            '''INSERT INTO stock_items
               (org_id, sku, name, marketplace, stock_qty, cost_price, sale_price,
                min_stock, avg_daily_sales, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (org_id, sku, name, mp,
             random.randint(5, 150),
             cost, sale,
             10,
             round(random.uniform(0.5, 8.0), 2),
             'ok' if random.random() > 0.15 else 'low')
        )
        count += 1
    return count


def seed_contacts(db, org_id, n=120):
    """Popula contatos (CRM)."""
    count = 0
    for i in range(n):
        name = random.choice(SAMPLE_NAMES) + f' {i+1}'
        email = f'cliente{i+1}@demo.sellvance.com.br'
        existing = db.execute(
            'SELECT id FROM contacts WHERE org_id=? AND email=?',
            (org_id, email)
        ).fetchone()
        if existing:
            continue
        ltv = round(random.uniform(50, 2500), 2)
        orders = random.randint(1, 12)
        days_ago = random.randint(1, 180)
        last_order = (datetime.now() - timedelta(days=random.randint(0, 120))).strftime('%Y-%m-%d %H:%M:%S')

        if orders >= 5 and ltv >= 800:
            rfm = 'champion'
        elif orders >= 3:
            rfm = 'loyal'
        elif days_ago > 90:
            rfm = 'at_risk'
        elif orders == 1:
            rfm = 'new'
        else:
            rfm = 'potential'

        db.execute(
            '''INSERT INTO contacts
               (org_id, name, email, phone, source, rfm_segment, ltv, total_orders,
                last_order_at, wa_opt_in, email_opt_in)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1)''',
            (org_id, name, email,
             f'11{random.randint(90000, 99999)}{random.randint(1000, 9999)}',
             random.choice(CHANNELS),
             rfm, ltv, orders, last_order)
        )
        count += 1
    return count


def seed_orders(db, org_id, n=380):
    """Popula pedidos espalhados nos ultimos 90 dias."""
    # Pega IDs de contacts
    contact_ids = [r['id'] for r in db.execute(
        'SELECT id FROM contacts WHERE org_id=? LIMIT 120', (org_id,)
    ).fetchall()]
    if not contact_ids:
        return 0

    count = 0
    for i in range(n):
        product = random.choice(SAMPLE_PRODUCTS)
        sku, pname = product[0], product[1]
        channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
        revenue = round(random.uniform(29, 599), 2)
        days_ago = random.randint(0, 90)
        created = _rand_date(days_ago, days_ago)
        contact_id = random.choice(contact_ids)

        try:
            db.execute(
                '''INSERT INTO orders
                   (org_id, contact_id, channel, sku, product_name, revenue,
                    status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'completed', ?)''',
                (org_id, contact_id, channel, sku, pname, revenue, created)
            )
            count += 1
        except Exception:
            continue  # schema pode variar
    return count


def seed_campaigns(db, org_id):
    """Popula campanhas de ads (reais, nao demos com padrao meta_N)."""
    campaigns = [
        ('meta', 'ABO_Conversion_SmartWatch_BR', 'active', 15000, 4500, 298000),
        ('meta', 'Lookalike_Champions_Premium', 'active', 8500, 2100, 180000),
        ('meta', 'Retargeting_Carrinho_Abandonado', 'active', 6200, 1800, 145000),
        ('meta', 'CBO_Eletronicos_Prospect', 'active', 12000, 3400, 220000),
        ('google', 'Search_Brand_Exact_Match', 'active', 4500, 1900, 95000),
        ('google', 'Shopping_TodaCategoria', 'active', 9800, 2800, 175000),
        ('google', 'Performance_Max_Conversoes', 'active', 11500, 3100, 198000),
        ('google', 'Display_Remarketing', 'active', 3200, 850, 45000),
    ]
    count = 0
    for i, c in enumerate(campaigns):
        platform, name, status, spend, conv, revenue = c
        ext_id = f'camp_real_{i+1}_{random.randint(10000, 99999)}'
        existing = db.execute(
            'SELECT id FROM ad_campaigns WHERE org_id=? AND external_campaign_id=?',
            (org_id, ext_id)
        ).fetchone()
        if existing:
            continue
        impressions = spend * random.randint(80, 200)
        clicks = int(impressions * random.uniform(0.015, 0.045))
        try:
            db.execute(
                '''INSERT INTO ad_campaigns
                   (org_id, platform, name, external_campaign_id, status,
                    spend, revenue, impressions, clicks, conversions, date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, date('now'))''',
                (org_id, platform, name, ext_id, status,
                 spend, revenue, impressions, clicks, conv)
            )
            count += 1
        except Exception:
            continue
    return count


def seed_competitor_prices(db, org_id):
    """Popula precos de competidores (para pricing_ai trabalhar)."""
    competitors = ['SuperVendas', 'MegaStore', 'TopLoja', 'PromoCenter', 'EcomPlus']
    count = 0
    for p in SAMPLE_PRODUCTS[:25]:  # primeiros 25 produtos
        sku, name, _, _, cost, sale, mp = p
        # 2-3 competidores por SKU
        num_comps = random.randint(2, 3)
        for j in range(num_comps):
            comp_name = random.choice(competitors)
            # Preco do competidor varia -15% a +10% do nosso sale
            comp_price = round(sale * random.uniform(0.85, 1.10), 2)
            has_bb = 1 if (j == 0 and random.random() > 0.55) else 0
            try:
                db.execute(
                    '''INSERT INTO competitor_prices
                       (org_id, sku, marketplace, competitor_id, competitor_name,
                        price, has_buybox)
                       VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (org_id, sku, mp, f'comp_{j}', comp_name, comp_price, has_bb)
                )
                count += 1
            except Exception:
                continue
    return count


def seed_buybox_snapshots(db, org_id):
    """Popula snapshots de Buy Box."""
    count = 0
    for p in SAMPLE_PRODUCTS[:15]:
        sku, name, _, _, cost, sale, mp = p
        has_bb = 1 if random.random() > 0.4 else 0
        comp_price = round(sale * random.uniform(0.92, 1.08), 2)
        try:
            db.execute(
                '''INSERT INTO buybox_snapshots
                   (org_id, marketplace, sku, product_title, has_buybox,
                    competitor_name, our_price, winner_price,
                    diff_pct)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (org_id, mp, sku, name, has_bb, 'SuperVendas',
                 sale, comp_price, round(((comp_price - sale) / sale) * 100, 2))
            )
            count += 1
        except Exception:
            continue
    return count


def seed_fraud_pending(db, org_id):
    """Cria alguns alertas de fraude pendentes."""
    cases = [
        ('ORD-9841', 'cust_12', 'Maria Silva', 'FONE-JBL-T110',
         'nao_gostei', 79.90, 68, 'review'),
        ('ORD-9842', 'cust_47', 'Joao Pereira', 'SMARTWATCH-MI-BAND8',
         'mudei_ideia', 239.00, 74, 'review'),
        ('ORD-9843', 'cust_88', 'Carlos Dias', 'TENIS-RUNNING-MASCULINO',
         'outro', 289.00, 55, 'review'),
    ]
    count = 0
    import json as _json
    for c in cases:
        try:
            db.execute(
                '''INSERT INTO fraud_scores
                   (org_id, order_id, customer_id, customer_name, sku,
                    return_reason, order_value, score, signals, decision)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (org_id, *c[:7],
                 _json.dumps({'customer_return_rate': 30, 'rapid_return': 15}),
                 c[7])
            )
            count += 1
        except Exception:
            continue
    return count


def seed_insights(db, org_id):
    """Cria alguns insights pendentes para mostrar no admin."""
    insights = [
        ('revenue', 'opportunity', '12 SKUs com Buy Box de competidor detectados',
         'Competidores estao ganhando Buy Box em produtos de alta margem',
         12.0, 'Ativar ai_pricing para recuperar Buy Box automaticamente'),
        ('engagement', 'warning', 'Taxa de conversao no carrinho caiu',
         'Conversao de carrinho para checkout caiu 15% nos ultimos 7 dias',
         15.0, 'Revisar flow de checkout e frete calculado'),
        ('revenue', 'opportunity', 'Campanhas Meta Ads com ROAS > 4x',
         '2 campanhas com ROAS acima de 4x e budget nao maximizado',
         4.2, 'Escalar budget dessas campanhas em 30%'),
    ]
    count = 0
    for i in insights:
        try:
            db.execute(
                '''INSERT INTO platform_insights
                   (category, severity, title, description, metric_value, recommendation)
                   VALUES (?, ?, ?, ?, ?, ?)''',
                i
            )
            count += 1
        except Exception:
            continue
    return count


def seed_all(org_id, force=False):
    """
    Popula toda a simulacao. Chama de forma idempotente.
    Se force=True, limpa antes (cuidado!).
    """
    random.seed(42 + org_id)  # deterministico por org
    db = get_db()

    if force:
        # Limpa dados da org (NAO usuario/org em si)
        for table in ('stock_items', 'contacts', 'orders', 'ad_campaigns',
                      'competitor_prices', 'buybox_snapshots', 'fraud_scores',
                      'platform_insights'):
            try:
                if table == 'platform_insights':
                    db.execute(f"DELETE FROM {table}")
                else:
                    db.execute(f"DELETE FROM {table} WHERE org_id=?", (org_id,))
            except Exception:
                pass

    result = {
        'products':       seed_products(db, org_id),
        'contacts':       seed_contacts(db, org_id, n=120),
        'orders':         seed_orders(db, org_id, n=380),
        'campaigns':      seed_campaigns(db, org_id),
        'competitor_prices': seed_competitor_prices(db, org_id),
        'buybox_snapshots':  seed_buybox_snapshots(db, org_id),
        'fraud_pending':  seed_fraud_pending(db, org_id),
        'insights':       seed_insights(db, org_id),
    }
    db.commit()
    db.close()

    try:
        from telemetry import track
        track('action', 'demo_data_seeded', **result)
    except Exception:
        pass

    return result


def clear_all(org_id):
    """Limpa todos os dados simulados (CUIDADO)."""
    db = get_db()
    for table in ('stock_items', 'contacts', 'orders', 'ad_campaigns',
                  'competitor_prices', 'buybox_snapshots', 'fraud_scores'):
        try:
            db.execute(f"DELETE FROM {table} WHERE org_id=?", (org_id,))
        except Exception:
            pass
    db.commit()
    db.close()
    return {'ok': True}
