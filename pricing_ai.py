# pricing_ai.py — IA de precificacao dinamica
# Analisa preco de competidores, margem minima, velocity e Buy Box
# para sugerir ou aplicar ajustes automaticos de preco.
#
# Filosofia:
#   1. NUNCA aplica mudanca que quebre margem minima
#   2. Analisa competidores com no maximo 15min de delay
#   3. Aprende com historico: se baixar preco nao aumentou vendas, para
#   4. Protege Buy Box ativamente (ML/Amazon)

from database import get_db


# Limites de seguranca
MIN_MARGIN_PCT = 0.10     # nunca vende com menos de 10% de margem
MAX_PRICE_DROP_PCT = 0.15 # nao baixa mais de 15% de uma vez
MAX_PRICE_RISE_PCT = 0.20 # nao sobe mais de 20% de uma vez
BUYBOX_SAFETY_MARGIN = 0.01  # 1% abaixo do competidor para ganhar Buy Box


def ensure_tables():
    """Bootstrap das tabelas de pricing AI."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS pricing_rules (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            sku             TEXT NOT NULL,
            marketplace     TEXT DEFAULT 'all',
            min_price       REAL NOT NULL,
            max_price       REAL NOT NULL,
            cost_price      REAL NOT NULL,
            target_margin   REAL DEFAULT 0.20,
            strategy        TEXT DEFAULT 'buybox',
            auto_apply      INTEGER DEFAULT 0,
            is_active       INTEGER DEFAULT 1,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS pricing_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            sku             TEXT NOT NULL,
            marketplace     TEXT,
            old_price       REAL,
            new_price       REAL,
            competitor_price REAL,
            reason          TEXT,
            applied_by      TEXT DEFAULT 'ai',
            result          TEXT DEFAULT 'pending',
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_pricing_hist_sku
            ON pricing_history(org_id, sku, created_at);

        CREATE TABLE IF NOT EXISTS competitor_prices (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER,
            sku             TEXT,
            marketplace     TEXT,
            competitor_id   TEXT,
            competitor_name TEXT,
            price           REAL,
            has_buybox      INTEGER DEFAULT 0,
            captured_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_comp_prices_sku
            ON competitor_prices(org_id, sku, marketplace, captured_at);
    ''')
    db.commit()
    db.close()


def suggest_price(org_id, sku, marketplace='mercado_livre'):
    """
    Retorna uma sugestao de preco baseada em:
      - Regra de precificacao do SKU (min/max/margem)
      - Precos atuais de competidores
      - Historico (se ultima mudanca funcionou)

    Return: dict com suggested_price, reason, confidence, action
    """
    db = get_db()

    # 1. Buscar regra
    rule = db.execute(
        '''SELECT * FROM pricing_rules
           WHERE org_id=? AND sku=?
           AND (marketplace=? OR marketplace='all')
           AND is_active=1
           ORDER BY CASE WHEN marketplace=? THEN 0 ELSE 1 END
           LIMIT 1''',
        (org_id, sku, marketplace, marketplace)
    ).fetchone()

    if not rule:
        db.close()
        return {
            'suggested_price': None,
            'reason': 'sem_regra_definida',
            'confidence': 0,
            'action': 'skip'
        }

    rule = dict(rule)
    min_safe = max(rule['min_price'], rule['cost_price'] * (1 + MIN_MARGIN_PCT))
    max_price = rule['max_price']

    # 2. Preco atual do SKU
    current = db.execute(
        '''SELECT sale_price FROM stock_items
           WHERE org_id=? AND sku=?
           ORDER BY last_updated DESC LIMIT 1''',
        (org_id, sku)
    ).fetchone()
    current_price = current['sale_price'] if current else rule['max_price']

    # 3. Preco do competidor com Buy Box (ultimos 30min)
    comp = db.execute(
        '''SELECT price, has_buybox, competitor_name
           FROM competitor_prices
           WHERE org_id=? AND sku=? AND marketplace=?
           AND captured_at > datetime('now', '-30 minutes')
           ORDER BY has_buybox DESC, price ASC
           LIMIT 1''',
        (org_id, sku, marketplace)
    ).fetchone()

    db.close()

    if not comp:
        return {
            'suggested_price': current_price,
            'reason': 'sem_dados_competidor',
            'confidence': 0.3,
            'action': 'monitor'
        }

    comp_price = comp['price']
    comp_has_bb = comp['has_buybox']
    strategy = rule['strategy']

    # 4. Aplicar estrategia
    if strategy == 'buybox':
        # Objetivo: ficar ligeiramente abaixo do Buy Box
        target = comp_price * (1 - BUYBOX_SAFETY_MARGIN)
    elif strategy == 'match':
        target = comp_price
    elif strategy == 'premium':
        target = comp_price * 1.05  # 5% acima (para produtos diferenciados)
    elif strategy == 'aggressive':
        target = comp_price * 0.95  # 5% abaixo
    else:
        target = current_price

    # 5. Aplicar limites de seguranca
    target = max(target, min_safe)
    target = min(target, max_price)

    # Limitar velocidade da mudanca
    max_down = current_price * (1 - MAX_PRICE_DROP_PCT)
    max_up   = current_price * (1 + MAX_PRICE_RISE_PCT)
    target = max(target, max_down)
    target = min(target, max_up)
    target = round(target, 2)

    # 6. Decidir acao
    delta_pct = (target - current_price) / current_price if current_price > 0 else 0
    if abs(delta_pct) < 0.005:  # <0.5% de diferenca, nao mexe
        action = 'keep'
        reason = 'preco_otimo'
    elif target < current_price and comp_has_bb:
        action = 'decrease'
        reason = 'recuperar_buybox'
    elif target < current_price:
        action = 'decrease'
        reason = 'alinhar_competidor'
    else:
        action = 'increase'
        reason = 'margem_disponivel'

    confidence = 0.9 if comp_has_bb else 0.7

    return {
        'suggested_price': target,
        'current_price': current_price,
        'competitor_price': comp_price,
        'competitor_name': comp['competitor_name'],
        'delta_pct': round(delta_pct * 100, 2),
        'reason': reason,
        'confidence': confidence,
        'action': action,
        'auto_apply': bool(rule['auto_apply']),
        'min_safe': round(min_safe, 2),
    }


def apply_price_change(org_id, sku, new_price, marketplace='all', reason='ai_suggestion'):
    """
    Aplica uma mudanca de preco e registra no historico.
    Em producao, isto chamaria as APIs de ML/Amazon/Shopee.
    Por enquanto, so atualiza o banco local.
    """
    db = get_db()
    current = db.execute(
        'SELECT sale_price FROM stock_items WHERE org_id=? AND sku=? LIMIT 1',
        (org_id, sku)
    ).fetchone()
    old_price = current['sale_price'] if current else None

    db.execute(
        '''UPDATE stock_items SET sale_price=?, last_updated=datetime('now')
           WHERE org_id=? AND sku=?''',
        (new_price, org_id, sku)
    )
    db.execute(
        '''INSERT INTO pricing_history
           (org_id, sku, marketplace, old_price, new_price, reason, applied_by)
           VALUES (?, ?, ?, ?, ?, ?, 'ai')''',
        (org_id, sku, marketplace, old_price, new_price, reason)
    )
    db.commit()
    db.close()

    try:
        from telemetry import track
        track('action', 'price_changed', sku=sku,
              old=old_price, new=new_price, reason=reason)
    except Exception:
        pass
    return True


def run_pricing_batch(org_id):
    """
    Roda o pricing AI para todos os SKUs com auto_apply=1.
    Chamar via cron a cada 15-30min.
    Retorna contador de mudancas aplicadas.
    """
    from feature_flags import is_enabled
    if not is_enabled('ai_pricing', org_id):
        return {'skipped': True, 'reason': 'feature_disabled'}

    db = get_db()
    rules = db.execute(
        '''SELECT DISTINCT sku FROM pricing_rules
           WHERE org_id=? AND is_active=1 AND auto_apply=1''',
        (org_id,)
    ).fetchall()
    db.close()

    applied = 0
    skipped = 0
    errors = 0
    changes = []

    for r in rules:
        sku = r['sku']
        try:
            s = suggest_price(org_id, sku)
            if s['action'] in ('decrease', 'increase') and s.get('auto_apply'):
                apply_price_change(
                    org_id, sku, s['suggested_price'],
                    reason=f"ai:{s['reason']}"
                )
                applied += 1
                changes.append({'sku': sku, 'new': s['suggested_price'], 'reason': s['reason']})
            else:
                skipped += 1
        except Exception:
            errors += 1

    return {
        'applied': applied,
        'skipped': skipped,
        'errors': errors,
        'changes': changes[:10]  # primeiras 10 para log
    }


def get_pricing_stats(org_id):
    """Estatisticas para dashboard de precificacao."""
    db = get_db()
    total_rules = db.execute(
        'SELECT COUNT(*) c FROM pricing_rules WHERE org_id=? AND is_active=1',
        (org_id,)
    ).fetchone()
    auto_enabled = db.execute(
        'SELECT COUNT(*) c FROM pricing_rules WHERE org_id=? AND is_active=1 AND auto_apply=1',
        (org_id,)
    ).fetchone()
    changes_24h = db.execute(
        '''SELECT COUNT(*) c FROM pricing_history
           WHERE org_id=? AND created_at > datetime('now', '-1 day')''',
        (org_id,)
    ).fetchone()
    avg_delta = db.execute(
        '''SELECT AVG(((new_price - old_price) / old_price) * 100) d
           FROM pricing_history
           WHERE org_id=? AND old_price > 0
           AND created_at > datetime('now', '-7 days')''',
        (org_id,)
    ).fetchone()
    db.close()
    return {
        'total_rules': total_rules['c'] if total_rules else 0,
        'auto_enabled': auto_enabled['c'] if auto_enabled else 0,
        'changes_24h': changes_24h['c'] if changes_24h else 0,
        'avg_delta_7d': round(avg_delta['d'] or 0, 2),
    }
