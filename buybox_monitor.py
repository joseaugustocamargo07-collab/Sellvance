# buybox_monitor.py — Monitoramento de Buy Box em tempo real
# Alerta quando o seller perde/ganha o Buy Box em Mercado Livre ou Amazon.
# Integra com pricing_ai para disparar re-precificacao automaticamente.

from database import get_db


# Status possiveis
STATUS_HAS_BUYBOX = 'has_buybox'
STATUS_LOST_BUYBOX = 'lost_buybox'
STATUS_NEVER_HAD = 'never_had'


def ensure_tables():
    """Bootstrap das tabelas de buy box monitoring."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS buybox_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            marketplace     TEXT NOT NULL,
            sku             TEXT NOT NULL,
            product_title   TEXT,
            has_buybox      INTEGER DEFAULT 0,
            competitor_name TEXT,
            our_price       REAL,
            winner_price    REAL,
            diff_pct        REAL,
            captured_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_buybox_sku
            ON buybox_snapshots(org_id, marketplace, sku, captured_at);

        CREATE TABLE IF NOT EXISTS buybox_alerts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            marketplace     TEXT NOT NULL,
            sku             TEXT NOT NULL,
            alert_type      TEXT NOT NULL,
            product_title   TEXT,
            competitor_name TEXT,
            our_price       REAL,
            winner_price    REAL,
            suggested_action TEXT,
            status          TEXT DEFAULT 'new',
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_buybox_alerts_status
            ON buybox_alerts(org_id, status, created_at);
    ''')
    db.commit()
    db.close()


def record_snapshot(org_id, marketplace, sku, has_buybox, **kwargs):
    """
    Registra um snapshot do estado do Buy Box.
    Detecta mudancas vs snapshot anterior e cria alerta se necessario.
    """
    db = get_db()

    # Ultimo snapshot desse SKU
    prev = db.execute(
        '''SELECT * FROM buybox_snapshots
           WHERE org_id=? AND marketplace=? AND sku=?
           ORDER BY id DESC LIMIT 1''',
        (org_id, marketplace, sku)
    ).fetchone()

    db.execute(
        '''INSERT INTO buybox_snapshots
           (org_id, marketplace, sku, product_title, has_buybox,
            competitor_name, our_price, winner_price, diff_pct)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (org_id, marketplace, sku,
         kwargs.get('product_title'),
         1 if has_buybox else 0,
         kwargs.get('competitor_name'),
         kwargs.get('our_price'),
         kwargs.get('winner_price'),
         kwargs.get('diff_pct'))
    )

    # Detectar mudanca
    alert_created = False
    if prev:
        prev_had = bool(prev['has_buybox'])
        if prev_had and not has_buybox:
            # Perdeu o Buy Box
            db.execute(
                '''INSERT INTO buybox_alerts
                   (org_id, marketplace, sku, alert_type, product_title,
                    competitor_name, our_price, winner_price, suggested_action)
                   VALUES (?, ?, ?, 'lost', ?, ?, ?, ?, ?)''',
                (org_id, marketplace, sku,
                 kwargs.get('product_title'),
                 kwargs.get('competitor_name'),
                 kwargs.get('our_price'),
                 kwargs.get('winner_price'),
                 f"Baixar preco para {kwargs.get('winner_price', 0) * 0.99:.2f}")
            )
            alert_created = True
            try:
                from telemetry import track
                track('alert', 'buybox_lost', sku=sku, marketplace=marketplace)
            except Exception:
                pass
        elif not prev_had and has_buybox:
            # Ganhou o Buy Box
            db.execute(
                '''INSERT INTO buybox_alerts
                   (org_id, marketplace, sku, alert_type, product_title,
                    our_price, suggested_action)
                   VALUES (?, ?, ?, 'won', ?, ?, ?)''',
                (org_id, marketplace, sku,
                 kwargs.get('product_title'),
                 kwargs.get('our_price'),
                 'Monitorar — considerar subir preco gradualmente')
            )
            alert_created = True
            try:
                from telemetry import track
                track('alert', 'buybox_won', sku=sku, marketplace=marketplace)
            except Exception:
                pass

    db.commit()
    db.close()

    # Se perdeu Buy Box e ai_pricing esta ativo, disparar re-precificacao
    if alert_created and not has_buybox:
        try:
            from feature_flags import is_enabled
            from pricing_ai import suggest_price, apply_price_change
            if is_enabled('ai_pricing', org_id):
                s = suggest_price(org_id, sku, marketplace)
                if s.get('action') == 'decrease' and s.get('suggested_price'):
                    apply_price_change(
                        org_id, sku, s['suggested_price'],
                        marketplace=marketplace,
                        reason='buybox_lost_auto_reprice'
                    )
        except Exception:
            pass

    return alert_created


def get_current_status(org_id, marketplace=None):
    """Retorna status atual de Buy Box para todos os SKUs."""
    db = get_db()
    sql = '''
        SELECT sku, marketplace, has_buybox, our_price, winner_price,
               competitor_name, product_title, MAX(captured_at) as last_check
        FROM buybox_snapshots
        WHERE org_id = ?
    '''
    params = [org_id]
    if marketplace:
        sql += ' AND marketplace = ?'
        params.append(marketplace)
    sql += ' GROUP BY sku, marketplace ORDER BY last_check DESC'
    rows = [dict(r) for r in db.execute(sql, params).fetchall()]
    db.close()
    return rows


def get_alerts(org_id, status='new', limit=50):
    """Lista alertas pendentes."""
    db = get_db()
    rows = [dict(r) for r in db.execute(
        '''SELECT * FROM buybox_alerts
           WHERE org_id=? AND status=?
           ORDER BY created_at DESC LIMIT ?''',
        (org_id, status, limit)
    ).fetchall()]
    db.close()
    return rows


def get_stats(org_id, days=7):
    """Estatisticas de Buy Box."""
    db = get_db()
    total_skus = db.execute(
        '''SELECT COUNT(DISTINCT sku) c FROM buybox_snapshots WHERE org_id=?''',
        (org_id,)
    ).fetchone()

    has_bb = db.execute('''
        SELECT COUNT(DISTINCT sku) c FROM (
            SELECT sku, has_buybox, MAX(captured_at) mx
            FROM buybox_snapshots WHERE org_id=?
            GROUP BY sku
        ) WHERE has_buybox = 1
    ''', (org_id,)).fetchone()

    lost_24h = db.execute(
        '''SELECT COUNT(*) c FROM buybox_alerts
           WHERE org_id=? AND alert_type='lost'
           AND created_at > datetime('now', '-1 day')''',
        (org_id,)
    ).fetchone()

    won_24h = db.execute(
        '''SELECT COUNT(*) c FROM buybox_alerts
           WHERE org_id=? AND alert_type='won'
           AND created_at > datetime('now', '-1 day')''',
        (org_id,)
    ).fetchone()

    db.close()
    total = total_skus['c'] if total_skus else 0
    with_bb = has_bb['c'] if has_bb else 0
    return {
        'total_skus': total,
        'with_buybox': with_bb,
        'without_buybox': total - with_bb,
        'buybox_rate': round((with_bb / max(total, 1)) * 100, 1),
        'lost_24h': lost_24h['c'] if lost_24h else 0,
        'won_24h': won_24h['c'] if won_24h else 0,
    }


def mark_alert_handled(alert_id):
    db = get_db()
    db.execute('UPDATE buybox_alerts SET status="handled" WHERE id=?', (alert_id,))
    db.commit()
    db.close()
