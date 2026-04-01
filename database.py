import sqlite3
import os
from auth import hash_password

# Railway usa /data como volume persistente; localmente usa o diretório atual
DATA_DIR = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(DATA_DIR, 'sellvance.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn

def init_db():
    if os.path.exists(DB_PATH):
        return  # já inicializado

    db = get_db()

    db.executescript('''
        CREATE TABLE IF NOT EXISTS organizations (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT NOT NULL,
            plan     TEXT DEFAULT 'growth'
        );

        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id        INTEGER REFERENCES organizations(id),
            org_name      TEXT,
            name          TEXT NOT NULL,
            email         TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at    TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS contacts (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id        INTEGER NOT NULL,
            name          TEXT NOT NULL,
            email         TEXT NOT NULL,
            phone         TEXT,
            source        TEXT DEFAULT 'manual',
            rfm_segment   TEXT DEFAULT 'new',
            ltv           REAL DEFAULT 0,
            total_orders  INTEGER DEFAULT 0,
            last_order_at TEXT,
            wa_opt_in     INTEGER DEFAULT 0,
            email_opt_in  INTEGER DEFAULT 1,
            created_at    TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS orders (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id      INTEGER NOT NULL,
            contact_id  INTEGER,
            marketplace TEXT NOT NULL,
            external_id TEXT,
            status      TEXT DEFAULT 'delivered',
            gmv         REAL DEFAULT 0,
            revenue     REAL DEFAULT 0,
            cost        REAL DEFAULT 0,
            channel     TEXT DEFAULT 'organic',
            ordered_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS ad_campaigns (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id               INTEGER NOT NULL,
            platform             TEXT NOT NULL,
            external_campaign_id TEXT,
            name                 TEXT NOT NULL,
            objective            TEXT DEFAULT 'conversao',
            audience             TEXT DEFAULT 'broad',
            spend                REAL DEFAULT 0,
            budget_daily         REAL DEFAULT 0,
            revenue              REAL DEFAULT 0,
            impressions          INTEGER DEFAULT 0,
            clicks               INTEGER DEFAULT 0,
            conversions          INTEGER DEFAULT 0,
            leads                INTEGER DEFAULT 0,
            reach                INTEGER DEFAULT 0,
            video_views          INTEGER DEFAULT 0,
            status               TEXT DEFAULT 'active',
            paused_by_ai         INTEGER DEFAULT 0,
            ai_note              TEXT DEFAULT '',
            date                 TEXT DEFAULT (date('now'))
        );

        CREATE TABLE IF NOT EXISTS campaign_daily (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER REFERENCES ad_campaigns(id),
            org_id      INTEGER NOT NULL,
            date        TEXT NOT NULL,
            spend       REAL DEFAULT 0,
            revenue     REAL DEFAULT 0,
            clicks      INTEGER DEFAULT 0,
            impressions INTEGER DEFAULT 0,
            conversions INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS api_integrations (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id       INTEGER NOT NULL,
            platform     TEXT NOT NULL,
            status       TEXT DEFAULT 'disconnected',
            account_id   TEXT,
            account_name TEXT,
            last_sync    TEXT,
            config_json  TEXT DEFAULT '{}',
            UNIQUE(org_id, platform)
        );
    ''')

    # ── Organização demo ──────────────────────────────────────
    db.execute("INSERT INTO organizations (name, plan) VALUES ('Primeplas Coolers', 'growth')")
    org_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

    # ── Usuário admin ─────────────────────────────────────────
    db.execute('''
        INSERT INTO users (org_id, org_name, name, email, password_hash)
        VALUES (?, 'Primeplas Coolers', 'José Augusto', 'admin@sellvance.com', ?)
    ''', (org_id, hash_password('sellvance123')))

    # ── Contatos demo ─────────────────────────────────────────
    contacts_data = [
        ('João Silva',       'joao@email.com',    '11999991111', 'mercado_livre', 'champion', 842.50, 8,  'whatsapp'),
        ('Maria Costa',      'maria@email.com',   '11999992222', 'amazon',        'loyal',    624.00, 5,  'email'),
        ('Pedro Alves',      'pedro@email.com',   '11999993333', 'tiktok_shop',   'new',      189.90, 1,  ''),
        ('Camila Ferreira',  'camila@email.com',  '11999994444', 'mercado_livre', 'at_risk',  280.00, 3,  'whatsapp'),
        ('Rafael Lima',      'rafael@email.com',  '11999995555', 'amazon',        'lost',     189.90, 2,  ''),
        ('Ana Beatriz',      'ana@email.com',     '11999996666', 'tiktok_shop',   'champion', 1240.00,12, 'whatsapp'),
        ('Carlos Eduardo',   'carlos@email.com',  '11999997777', 'mercado_livre', 'loyal',    520.00, 6,  'email'),
        ('Fernanda Souza',   'fernanda@email.com','11999998888', 'amazon',        'potential',310.00, 2,  ''),
        ('Lucas Mendes',     'lucas@email.com',   '11999990000', 'tiktok_shop',   'new',      189.90, 1,  'whatsapp'),
        ('Juliana Rocha',    'juliana@email.com', '11988881111', 'mercado_livre', 'at_risk',  420.00, 4,  'email'),
        ('Ricardo Nunes',    'ricardo@email.com', '11988882222', 'amazon',        'loyal',    680.00, 7,  'whatsapp'),
        ('Patricia Lemos',   'patricia@email.com','11988883333', 'tiktok_shop',   'champion', 980.00, 9,  'email'),
        ('Diego Castro',     'diego@email.com',   '11988884444', 'mercado_livre', 'lost',     95.00,  1,  ''),
        ('Vanessa Lima',     'vanessa@email.com', '11988885555', 'amazon',        'new',      189.90, 1,  'whatsapp'),
        ('Marcelo Santos',   'marcelo@email.com', '11988886666', 'tiktok_shop',   'loyal',    740.00, 6,  'email'),
    ]

    contact_ids = []
    for c in contacts_data:
        wa = 1 if c[7] == 'whatsapp' else 0
        em = 1 if c[7] == 'email' else 0
        db.execute('''
            INSERT INTO contacts
              (org_id,name,email,phone,source,rfm_segment,ltv,total_orders,wa_opt_in,email_opt_in,last_order_at)
            VALUES (?,?,?,?,?,?,?,?,?,?, datetime('now', ? || ' days'))
        ''', (org_id, c[0], c[1], c[2], c[3], c[4], c[5], c[6], wa, em,
              str(-5 if c[4]=='champion' else -25 if c[4]=='loyal' else -3 if c[4]=='new'
                  else -70 if c[4]=='at_risk' else -120 if c[4]=='lost' else -40)))
        contact_ids.append(db.execute("SELECT last_insert_rowid()").fetchone()[0])

    # ── Pedidos demo (últimos 30 dias) ────────────────────────
    import random
    channels = ['meta_ads','google_ads','tiktok_ads','organic','email','whatsapp']
    marketplaces = ['mercado_livre','amazon','tiktok_shop']
    for i in range(120):
        cid = random.choice(contact_ids)
        contact = db.execute('SELECT * FROM contacts WHERE id=?',(cid,)).fetchone()
        gmv = random.choice([189.90, 139.90, 219.90, 259.90, 299.90])
        revenue = round(gmv * random.uniform(0.72, 0.82), 2)
        cost    = round(gmv * 0.36, 2)
        days_ago = random.randint(0, 29)
        db.execute('''
            INSERT INTO orders (org_id,contact_id,marketplace,status,gmv,revenue,cost,channel,ordered_at)
            VALUES (?,?,?,?,?,?,?,?, datetime('now', ? || ' days'))
        ''', (org_id, cid, contact['source'], 'delivered', gmv, revenue, cost,
              random.choice(channels), str(-days_ago)))

    # ── Campanhas de ads demo (ricas) ────────────────────────
    campaigns = [
        # (platform, ext_id, name, objective, audience, spend, budget_daily, revenue, impressions, clicks, conversions, leads, reach, video_views, status, paused_by_ai, ai_note)
        ('meta',   'meta_1', 'Cooler 32L — Conversão Fria',    'conversao',  'lookalike_1pct', 4200, 200, 21840, 280000, 8400,  115, 0,   210000, 0,      'active',  0, ''),
        ('meta',   'meta_2', 'Lookalike Compradores 2%',        'conversao',  'lookalike_2pct', 2800, 150, 12880, 190000, 5700,   78, 0,   145000, 0,      'active',  0, ''),
        ('meta',   'meta_3', 'Remarketing Visitantes 7d',       'conversao',  'remarketing',    1400, 100, 11340, 120000, 3600,   58, 0,    92000, 0,      'active',  0, ''),
        ('meta',   'meta_4', 'Topo Funil — Vídeo Verão',        'video',      'broad_25_45',     890,  80,  1200, 620000, 4200,    8, 0,   480000, 185000, 'active',  0, ''),
        ('meta',   'meta_5', 'Leads WhatsApp — Cooler',         'lead_gen',   'interesse_camping',980, 90,  2940, 210000, 6300,   0, 98,   168000, 0,      'active',  0, ''),
        ('meta',   'meta_6', 'Concorrentes — Caixa Térmica',    'conversao',  'interesse_cooler',320, 50,   480,  48000, 1440,    6, 0,    38000, 0,      'active',  0, ''),
        ('google', 'goog_1', 'Shopping — Cooler Caixa Térmica', 'shopping',   'smart',          3800, 180, 22040, 180000, 5400,  98, 0,        0, 0,      'active',  0, ''),
        ('google', 'goog_2', 'Search Branded — Primeplas',      'search',     'branded',         890,  60, 10680,  48000, 2340,  44, 0,        0, 0,      'active',  0, ''),
        ('google', 'goog_3', 'Search Genérico — Cooler',        'search',     'generico',       1200,  80,  3480,  62000, 1860,  22, 0,        0, 0,      'active',  0, ''),
        ('google', 'goog_4', 'PMax — Catálogo Completo',        'pmax',       'smart',          2100, 120, 14700,  95000, 3800,  70, 0,        0, 0,      'active',  0, ''),
        ('google', 'goog_5', 'Display Remarketing',             'display',    'remarketing',     480,  40,   720,  390000, 1200,  9, 0,        0, 0,      'active',  0, ''),
        ('tiktok', 'tik_1',  'TikTok Shop — Boost Cooler 32L', 'shop',       'broad',          2100, 120, 15750, 420000,12600, 210, 0,   380000, 290000, 'active',  0, ''),
        ('tiktok', 'tik_2',  'Viral Verão — Unboxing',          'video',      'interesse_18_35', 1800, 100, 13500, 380000,11400, 188, 0,   340000, 260000, 'active',  0, ''),
        ('tiktok', 'tik_3',  'Afiliados Top Creators',          'shop',       'afiliados',      1200,  80,  9000, 290000, 8700, 145, 0,   260000, 210000, 'active',  0, ''),
    ]
    campaign_ids = []
    for c in campaigns:
        db.execute('''
            INSERT INTO ad_campaigns
              (org_id,platform,external_campaign_id,name,objective,audience,
               spend,budget_daily,revenue,impressions,clicks,conversions,leads,reach,video_views,status,paused_by_ai,ai_note)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (org_id, *c))
        campaign_ids.append(db.execute("SELECT last_insert_rowid()").fetchone()[0])

    # ── Histórico diário das campanhas (últimos 14 dias) ──────
    import random
    for cid_idx, cid in enumerate(campaign_ids):
        camp = campaigns[cid_idx]
        base_spend   = camp[6] / 30
        base_revenue = camp[8] / 30
        base_clicks  = camp[10] / 30
        base_impr    = camp[9] / 30
        base_conv    = camp[11] / 30
        for d in range(14, 0, -1):
            var = random.uniform(0.7, 1.4)
            db.execute('''
                INSERT INTO campaign_daily (campaign_id, org_id, date, spend, revenue, clicks, impressions, conversions)
                VALUES (?, ?, date('now', ? || ' days'), ?, ?, ?, ?, ?)
            ''', (cid, org_id, str(-d),
                  round(base_spend * var, 2),
                  round(base_revenue * var, 2),
                  int(base_clicks * var),
                  int(base_impr * var),
                  max(0, int(base_conv * var))))

    db.commit()
    db.close()
    print("✅ Banco inicializado com dados demo!")

def migrate_db():
    """Adiciona tabelas novas sem recriar o banco."""
    db = get_db()
    existing = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    db.close()

    if 'whatsapp_campaigns' not in existing:
        db = get_db()
        db.executescript('''
            CREATE TABLE whatsapp_campaigns (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id      INTEGER NOT NULL,
                name        TEXT NOT NULL,
                segment     TEXT DEFAULT 'all',
                message     TEXT NOT NULL,
                status      TEXT DEFAULT 'draft',
                sent        INTEGER DEFAULT 0,
                delivered   INTEGER DEFAULT 0,
                read_count  INTEGER DEFAULT 0,
                replied     INTEGER DEFAULT 0,
                converted   INTEGER DEFAULT 0,
                revenue     REAL DEFAULT 0,
                scheduled_at TEXT,
                sent_at     TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE email_campaigns (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id      INTEGER NOT NULL,
                name        TEXT NOT NULL,
                subject     TEXT NOT NULL,
                segment     TEXT DEFAULT 'all',
                body_html   TEXT,
                status      TEXT DEFAULT 'draft',
                sent        INTEGER DEFAULT 0,
                delivered   INTEGER DEFAULT 0,
                opened      INTEGER DEFAULT 0,
                clicked     INTEGER DEFAULT 0,
                converted   INTEGER DEFAULT 0,
                unsubscribed INTEGER DEFAULT 0,
                revenue     REAL DEFAULT 0,
                scheduled_at TEXT,
                sent_at     TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE stock_items (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id          INTEGER NOT NULL,
                sku             TEXT NOT NULL,
                name            TEXT NOT NULL,
                marketplace     TEXT DEFAULT 'all',
                stock_qty       INTEGER DEFAULT 0,
                reserved_qty    INTEGER DEFAULT 0,
                min_stock       INTEGER DEFAULT 10,
                cost_price      REAL DEFAULT 0,
                sale_price      REAL DEFAULT 0,
                avg_daily_sales REAL DEFAULT 0,
                days_remaining  INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'ok',
                last_updated    TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS api_integrations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id      INTEGER NOT NULL,
                platform    TEXT NOT NULL,
                status      TEXT DEFAULT 'disconnected',
                account_id  TEXT,
                account_name TEXT,
                last_sync   TEXT,
                config_json TEXT DEFAULT '{}'
            );
        ''')

    if 'mini_loja_config' not in existing:
        db = get_db()
        db.executescript('''
            CREATE TABLE IF NOT EXISTS mini_loja_config (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id       INTEGER NOT NULL UNIQUE,
                slug         TEXT NOT NULL UNIQUE,
                store_name   TEXT NOT NULL DEFAULT '',
                logo_url     TEXT DEFAULT '',
                whatsapp     TEXT DEFAULT '',
                accent_color TEXT DEFAULT '#6c63ff',
                banner_text  TEXT DEFAULT '',
                is_active    INTEGER DEFAULT 0,
                created_at   TEXT DEFAULT (datetime('now')),
                updated_at   TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS mini_loja_products (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id        INTEGER NOT NULL,
                mp_product_id INTEGER NOT NULL,
                is_visible    INTEGER DEFAULT 1,
                sort_order    INTEGER DEFAULT 0,
                UNIQUE(org_id, mp_product_id)
            );
            CREATE TABLE IF NOT EXISTS mini_loja_analytics (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id      INTEGER NOT NULL,
                event_type  TEXT NOT NULL,
                product_id  INTEGER,
                ip_hash     TEXT,
                referrer    TEXT DEFAULT '',
                created_at  TEXT DEFAULT (datetime('now'))
            );
        ''')
        db.commit()
        db.close()

    if 'vulnerability_scores' not in existing:
        db = get_db()
        db.executescript('''
            CREATE TABLE IF NOT EXISTS vulnerability_scores (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id               INTEGER NOT NULL,
                platform             TEXT NOT NULL,
                external_id          TEXT,
                product_title        TEXT DEFAULT '',
                score                INTEGER DEFAULT 0,
                is_commodity         INTEGER DEFAULT 0,
                price_vulnerable     INTEGER DEFAULT 0,
                china_manufacturable INTEGER DEFAULT 0,
                delivery_advantage   INTEGER DEFAULT 0,
                brand_strength       INTEGER DEFAULT 0,
                factors_json         TEXT DEFAULT '{}',
                recommendations_json TEXT DEFAULT '[]',
                last_calculated      TEXT DEFAULT (datetime('now')),
                UNIQUE(org_id, platform, external_id)
            );

            CREATE TABLE IF NOT EXISTS vulnerability_alerts (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id          INTEGER NOT NULL,
                platform        TEXT NOT NULL,
                product_ext_id  TEXT,
                alert_type      TEXT DEFAULT 'new_competitor',
                title           TEXT NOT NULL,
                description     TEXT DEFAULT '',
                severity        TEXT DEFAULT 'medium',
                competitor_name TEXT DEFAULT '',
                competitor_price REAL DEFAULT 0,
                is_read         INTEGER DEFAULT 0,
                created_at      TEXT DEFAULT (datetime('now'))
            );
        ''')
        db.commit()
        db.close()

        # Seed WhatsApp campaigns
        org_id = db.execute('SELECT id FROM organizations LIMIT 1').fetchone()[0]
        wa_camps = [
            (org_id, 'Reativação Champions — Cooler 32L', 'champion',
             '🧊 Olá {nome}! Você é um dos nossos clientes especiais.\n\nTemos uma oferta exclusiva do Cooler 32L com *15% OFF* só para você!\n\n👉 Use o cupom: CHAMPION15\n⏰ Válido até amanhã!\n\nCompre agora: primeplas.com.br/cooler32l',
             'sent', 98, 91, 78, 42, 31, 5890.00, None, datetime_ago(-3)),
            (org_id, 'Recuperação At Risk — Oferta Relâmpago', 'at_risk',
             '⚡ {nome}, sentimos sua falta!\n\nFaz um tempo que você não nos visita. Preparamos uma oferta especial:\n\n*Cooler 20L por R$ 139,90* (de R$ 189,90)\n\nGaranta o seu 👇\nprimeplas.com.br/cooler20l',
             'sent', 45, 40, 29, 18, 12, 1678.80, None, datetime_ago(-7)),
            (org_id, 'Black Friday Antecipada — Base Toda', 'all',
             '🔥 BLACK FRIDAY ANTECIPADA!\n\nOlá {nome}! Chegou o momento que você esperava:\n\n🧊 Cooler 32L — de R$219,90 por *R$174,90*\n🧊 Cooler 20L — de R$189,90 por *R$139,90*\n\n⏰ Apenas 48h!\nprimeplas.com.br/blackfriday',
             'scheduled', 0, 0, 0, 0, 0, 0, datetime_ago(2), None),
            (org_id, 'Pós-compra — Avaliação + Upsell', 'new',
             '😊 Olá {nome}! Como está seu Cooler?\n\nEsperamos que esteja adorando! Conta pra gente: ⭐⭐⭐⭐⭐\n\nClientes que avaliaram ganham *10% OFF* na próxima compra!\n\nAvaliar agora: primeplas.com.br/avalie',
             'sent', 23, 21, 17, 9, 4, 559.60, None, datetime_ago(-1)),
        ]
        for w in wa_camps:
            db.execute('''INSERT INTO whatsapp_campaigns
                (org_id,name,segment,message,status,sent,delivered,read_count,replied,converted,revenue,scheduled_at,sent_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', w)

        # Seed Email campaigns
        em_camps = [
            (org_id, 'Newsletter Junho — Dicas de Verão', 'Dicas de Verão + Oferta Especial Primeplas',
             'loyal', 'sent', 1240, 1180, 486, 142, 38, 12, 7220.00, None, datetime_ago(-5)),
            (org_id, 'Carrinho Abandonado — Recuperação', 'Você esqueceu seu Cooler 🧊',
             'at_risk', 'sent', 89, 84, 62, 41, 18, 3, 3420.00, None, datetime_ago(-2)),
            (org_id, 'Onboarding Novos Clientes', 'Bem-vindo à família Primeplas! 🎉',
             'new', 'sent', 156, 148, 131, 67, 22, 1, 4180.00, None, datetime_ago(-10)),
            (org_id, 'Reengajamento 90 dias', 'Sentimos sua falta, {nome}',
             'lost', 'scheduled', 0, 0, 0, 0, 0, 0, 0, datetime_ago(5), None),
        ]
        for e in em_camps:
            db.execute('''INSERT INTO email_campaigns
                (org_id,name,subject,segment,status,sent,delivered,opened,clicked,converted,unsubscribed,revenue,scheduled_at,sent_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', e)

        # Seed Stock
        skus = [
            (org_id, 'PPL-32L-AZ', 'Cooler 32L Azul', 'mercado_livre', 48, 12, 15, 89.90, 219.90, 4.2, 'ok'),
            (org_id, 'PPL-32L-PT', 'Cooler 32L Preto', 'amazon',        22,  8, 15, 89.90, 219.90, 3.1, 'ok'),
            (org_id, 'PPL-20L-AZ', 'Cooler 20L Azul', 'mercado_livre', 9,   4, 15, 64.90, 189.90, 2.8, 'low'),
            (org_id, 'PPL-20L-PT', 'Cooler 20L Preto', 'tiktok_shop',   3,   2, 10, 64.90, 189.90, 3.5, 'critical'),
            (org_id, 'PPL-32L-VM', 'Cooler 32L Vermelho', 'amazon',     67,  5, 15, 89.90, 219.90, 2.0, 'ok'),
            (org_id, 'PPL-20L-VM', 'Cooler 20L Vermelho', 'tiktok_shop',0,   0, 10, 64.90, 189.90, 3.2, 'out'),
        ]
        for s in skus:
            qty, reserved, min_s, cost, price, daily, status = s[4], s[5], s[6], s[7], s[8], s[9], s[10]
            avail = qty - reserved
            days = int(avail / daily) if daily > 0 else 999
            db.execute('''INSERT INTO stock_items
                (org_id,sku,name,marketplace,stock_qty,reserved_qty,min_stock,cost_price,sale_price,avg_daily_sales,days_remaining,status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                (s[0], s[1], s[2], s[3], qty, reserved, min_s, cost, price, daily, days, status))

        # Seed API integrations
        integrations = [
            (org_id, 'meta_ads', 'disconnected', None, None),
            (org_id, 'google_ads', 'disconnected', None, None),
            (org_id, 'tiktok_ads', 'disconnected', None, None),
            (org_id, 'mercado_livre', 'connected', 'ML-123456', 'Primeplas Coolers'),
            (org_id, 'amazon', 'connected', 'AMZ-789012', 'Primeplas BR'),
            (org_id, 'tiktok_shop', 'connected', 'TTS-345678', 'PrimeplasShop'),
        ]
        for i in integrations:
            db.execute('INSERT INTO api_integrations (org_id,platform,status,account_id,account_name) VALUES (?,?,?,?,?)', i)

        db.commit()
        print('✅ Migração concluída!')
    else:
        print('✅ Banco já migrado.')
    db.close()

def datetime_ago(days):
    from datetime import datetime, timedelta
    return (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
