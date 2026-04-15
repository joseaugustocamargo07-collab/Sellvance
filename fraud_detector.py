# fraud_detector.py — Deteccao de fraude em devolucoes
# Analisa padroes de comportamento do cliente e do produto para flagear
# devolucoes suspeitas antes de aprovar o reembolso.
#
# Sinais analisados:
#   1. Taxa de devolucao historica do cliente (>40% = red flag)
#   2. Velocidade entre compra e devolucao (<24h = suspeito)
#   3. Valor do pedido vs ticket medio do cliente
#   4. SKU com alto taxa de fraude historica
#   5. Multiplas devolucoes na mesma janela
#   6. Motivo da devolucao inconsistente

from database import get_db


# Pesos dos sinais (somam 100)
SIGNAL_WEIGHTS = {
    'customer_return_rate': 30,   # cliente devolve muito
    'rapid_return':          15,  # devolveu muito rapido
    'value_outlier':         15,  # pedido muito acima do normal
    'sku_fraud_history':     20,  # SKU historicamente problematico
    'multi_returns_window':  15,  # varias devolucoes em pouco tempo
    'reason_inconsistency':   5,  # motivos incoerentes
}

# Thresholds de score
SCORE_LOW = 30    # < 30 = liberar automaticamente
SCORE_HIGH = 70   # >= 70 = bloquear e revisar
# Entre 30-70 = revisar manualmente


def ensure_tables():
    """Bootstrap das tabelas de fraud detection."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS fraud_scores (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            order_id        TEXT,
            customer_id     TEXT,
            customer_name   TEXT,
            sku             TEXT,
            return_reason   TEXT,
            order_value     REAL,
            score           INTEGER DEFAULT 0,
            signals         TEXT DEFAULT '{}',
            decision        TEXT DEFAULT 'pending',
            reviewed_by     TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_fraud_scores_decision
            ON fraud_scores(org_id, decision, created_at);

        CREATE TABLE IF NOT EXISTS customer_fraud_history (
            customer_id     TEXT PRIMARY KEY,
            org_id          INTEGER,
            total_orders    INTEGER DEFAULT 0,
            total_returns   INTEGER DEFAULT 0,
            return_rate     REAL DEFAULT 0,
            avg_ticket      REAL DEFAULT 0,
            last_return_at  TEXT,
            risk_label      TEXT DEFAULT 'normal',
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS sku_fraud_stats (
            sku             TEXT,
            org_id          INTEGER,
            total_sold      INTEGER DEFAULT 0,
            total_returned  INTEGER DEFAULT 0,
            return_rate     REAL DEFAULT 0,
            fraud_flagged   INTEGER DEFAULT 0,
            updated_at      TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (sku, org_id)
        );
    ''')
    db.commit()
    db.close()


def _get_customer_return_rate(db, org_id, customer_id):
    row = db.execute(
        'SELECT return_rate, total_orders FROM customer_fraud_history WHERE customer_id=? AND org_id=?',
        (customer_id, org_id)
    ).fetchone()
    if row and row['total_orders'] >= 3:
        return float(row['return_rate'])
    return 0.0


def _get_sku_fraud_rate(db, org_id, sku):
    row = db.execute(
        'SELECT return_rate FROM sku_fraud_stats WHERE sku=? AND org_id=?',
        (sku, org_id)
    ).fetchone()
    return float(row['return_rate']) if row else 0.0


def _get_multi_returns_count(db, org_id, customer_id, hours=72):
    row = db.execute(
        '''SELECT COUNT(*) c FROM fraud_scores
           WHERE org_id=? AND customer_id=?
           AND created_at > datetime('now', ?)''',
        (org_id, customer_id, f'-{hours} hours')
    ).fetchone()
    return row['c'] if row else 0


def score_return(org_id, order_data):
    """
    Analisa uma solicitacao de devolucao e retorna score + decisao.

    order_data = {
        'order_id': '123',
        'customer_id': 'abc',
        'customer_name': 'Joao',
        'sku': 'SKU001',
        'order_value': 150.0,
        'hours_since_purchase': 36,
        'return_reason': 'nao_gostei',
        'customer_avg_ticket': 80.0,
    }

    Return: {score, decision, signals, reasons}
    """
    db = get_db()
    signals = {}
    reasons = []

    # 1. Customer return rate
    rate = _get_customer_return_rate(db, org_id, order_data.get('customer_id'))
    if rate > 0.40:
        signals['customer_return_rate'] = SIGNAL_WEIGHTS['customer_return_rate']
        reasons.append(f'Cliente tem {rate*100:.0f}% de taxa de devolucao historica')
    elif rate > 0.25:
        signals['customer_return_rate'] = SIGNAL_WEIGHTS['customer_return_rate'] * 0.5
        reasons.append(f'Cliente tem taxa moderada ({rate*100:.0f}%)')

    # 2. Rapid return
    hours = order_data.get('hours_since_purchase', 999)
    if hours < 24:
        signals['rapid_return'] = SIGNAL_WEIGHTS['rapid_return']
        reasons.append(f'Devolucao em {hours}h apos compra (suspeito)')

    # 3. Value outlier
    avg_ticket = order_data.get('customer_avg_ticket', 0)
    order_value = order_data.get('order_value', 0)
    if avg_ticket > 0 and order_value > avg_ticket * 3:
        signals['value_outlier'] = SIGNAL_WEIGHTS['value_outlier']
        reasons.append(f'Pedido (R${order_value:.0f}) e 3x maior que ticket medio (R${avg_ticket:.0f})')

    # 4. SKU fraud history
    sku_rate = _get_sku_fraud_rate(db, org_id, order_data.get('sku'))
    if sku_rate > 0.15:
        signals['sku_fraud_history'] = SIGNAL_WEIGHTS['sku_fraud_history']
        reasons.append(f'SKU com {sku_rate*100:.0f}% de devolucao historica')

    # 5. Multi returns window
    recent = _get_multi_returns_count(db, org_id, order_data.get('customer_id'))
    if recent >= 3:
        signals['multi_returns_window'] = SIGNAL_WEIGHTS['multi_returns_window']
        reasons.append(f'{recent} devolucoes nas ultimas 72h')

    # 6. Reason inconsistency (stub simples)
    reason = (order_data.get('return_reason') or '').lower()
    if reason in ('mudei_ideia', 'nao_gostei', 'outro') and hours < 48:
        signals['reason_inconsistency'] = SIGNAL_WEIGHTS['reason_inconsistency']

    score = int(sum(signals.values()))

    if score >= SCORE_HIGH:
        decision = 'block'
    elif score >= SCORE_LOW:
        decision = 'review'
    else:
        decision = 'approve'

    # Gravar
    import json as _json
    db.execute(
        '''INSERT INTO fraud_scores
           (org_id, order_id, customer_id, customer_name, sku,
            return_reason, order_value, score, signals, decision)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (org_id, order_data.get('order_id'),
         order_data.get('customer_id'), order_data.get('customer_name'),
         order_data.get('sku'), order_data.get('return_reason'),
         order_value, score, _json.dumps(signals), decision)
    )
    db.commit()
    db.close()

    try:
        from telemetry import track
        track('action', 'fraud_scored',
              score=score, decision=decision, order_id=order_data.get('order_id'))
    except Exception:
        pass

    return {
        'score': score,
        'decision': decision,
        'signals': signals,
        'reasons': reasons,
        'thresholds': {'low': SCORE_LOW, 'high': SCORE_HIGH},
    }


def update_customer_history(org_id, customer_id, is_return=False, ticket=0):
    """Atualiza estatisticas historicas do cliente apos um pedido/devolucao."""
    db = get_db()
    existing = db.execute(
        'SELECT * FROM customer_fraud_history WHERE customer_id=? AND org_id=?',
        (customer_id, org_id)
    ).fetchone()

    if existing:
        e = dict(existing)
        new_orders = e['total_orders'] + 1
        new_returns = e['total_returns'] + (1 if is_return else 0)
        new_rate = new_returns / max(new_orders, 1)
        # Running average do ticket
        new_avg = ((e['avg_ticket'] * e['total_orders']) + ticket) / new_orders

        risk = 'high' if new_rate > 0.40 else ('medium' if new_rate > 0.25 else 'normal')

        db.execute(
            '''UPDATE customer_fraud_history
               SET total_orders=?, total_returns=?, return_rate=?,
                   avg_ticket=?, risk_label=?, updated_at=datetime('now'),
                   last_return_at = CASE WHEN ? THEN datetime('now') ELSE last_return_at END
               WHERE customer_id=? AND org_id=?''',
            (new_orders, new_returns, new_rate, new_avg, risk, is_return, customer_id, org_id)
        )
    else:
        db.execute(
            '''INSERT INTO customer_fraud_history
               (customer_id, org_id, total_orders, total_returns, return_rate,
                avg_ticket, last_return_at, risk_label)
               VALUES (?, ?, 1, ?, ?, ?, ?, 'normal')''',
            (customer_id, org_id, 1 if is_return else 0,
             1.0 if is_return else 0.0, ticket,
             'datetime("now")' if is_return else None)
        )
    db.commit()
    db.close()


def get_pending_reviews(org_id, limit=50):
    """Lista devolucoes aguardando revisao manual."""
    db = get_db()
    rows = [dict(r) for r in db.execute(
        '''SELECT * FROM fraud_scores
           WHERE org_id=? AND decision='review'
           ORDER BY score DESC, created_at DESC LIMIT ?''',
        (org_id, limit)
    ).fetchall()]
    db.close()
    return rows


def resolve_review(fraud_score_id, decision, reviewer='admin'):
    """Marca uma revisao como resolvida (approved ou blocked)."""
    db = get_db()
    db.execute(
        '''UPDATE fraud_scores
           SET decision=?, reviewed_by=?
           WHERE id=?''',
        (decision, reviewer, fraud_score_id)
    )
    db.commit()
    db.close()


def get_stats(org_id, days=30):
    """Estatisticas de deteccao de fraude."""
    db = get_db()
    total = db.execute(
        '''SELECT COUNT(*) c FROM fraud_scores
           WHERE org_id=? AND created_at > datetime('now', ?)''',
        (org_id, f'-{days} days')
    ).fetchone()
    blocked = db.execute(
        '''SELECT COUNT(*) c FROM fraud_scores
           WHERE org_id=? AND decision='block'
           AND created_at > datetime('now', ?)''',
        (org_id, f'-{days} days')
    ).fetchone()
    reviewing = db.execute(
        "SELECT COUNT(*) c FROM fraud_scores WHERE org_id=? AND decision='review'",
        (org_id,)
    ).fetchone()
    saved = db.execute(
        '''SELECT COALESCE(SUM(order_value), 0) s FROM fraud_scores
           WHERE org_id=? AND decision='block'
           AND created_at > datetime('now', ?)''',
        (org_id, f'-{days} days')
    ).fetchone()
    db.close()
    return {
        'total_analyzed': total['c'] if total else 0,
        'blocked': blocked['c'] if blocked else 0,
        'pending_review': reviewing['c'] if reviewing else 0,
        'value_saved': round(saved['s'] if saved else 0, 2),
        'period_days': days,
    }
