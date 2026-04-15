# auto_insights.py — Analise periodica da plataforma
# Roda a cada 6h e analisa telemetria/eventos para identificar:
#   - Rotas com alta latencia (candidatas a otimizacao)
#   - Features subutilizadas (candidatas a remocao ou melhor UI)
#   - Erros recorrentes
#   - Oportunidades de upsell (baseado em padrao de uso)
#
# Gera registros em platform_insights que o admin pode revisar.

from database import get_db


def ensure_tables():
    """Bootstrap da tabela de insights."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS platform_insights (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            category     TEXT NOT NULL,
            severity     TEXT DEFAULT 'info',
            title        TEXT NOT NULL,
            description  TEXT,
            metric_value REAL,
            recommendation TEXT,
            status       TEXT DEFAULT 'new',
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_insights_status
            ON platform_insights(status, created_at);
    ''')
    db.commit()
    db.close()


def _insert_insight(category, severity, title, desc, metric, recommendation):
    db = get_db()
    # Evita duplicatas nas ultimas 24h
    existing = db.execute(
        '''SELECT id FROM platform_insights
           WHERE category=? AND title=?
           AND created_at > datetime('now', '-1 day')''',
        (category, title)
    ).fetchone()
    if existing:
        db.close()
        return
    db.execute(
        '''INSERT INTO platform_insights
           (category, severity, title, description, metric_value, recommendation)
           VALUES (?, ?, ?, ?, ?, ?)''',
        (category, severity, title, desc, metric, recommendation)
    )
    db.commit()
    db.close()


def analyze_latency():
    """Identifica rotas lentas."""
    try:
        db = get_db()
        rows = db.execute('''
            SELECT page, AVG(duration_ms) avg_ms, COUNT(*) n
            FROM events_log
            WHERE event_type='request'
            AND created_at > datetime('now', '-6 hours')
            GROUP BY page
            HAVING n > 20 AND avg_ms > 2000
            ORDER BY avg_ms DESC
            LIMIT 5
        ''').fetchall()
        db.close()
        for r in rows:
            _insert_insight(
                'performance', 'warning',
                f"Rota lenta: {r['page']}",
                f"Media de {int(r['avg_ms'])}ms em {r['n']} requests nas ultimas 6h",
                float(r['avg_ms']),
                f"Adicionar cache ou otimizar queries em {r['page']}"
            )
    except Exception:
        pass


def analyze_errors():
    """Identifica rotas com alto error rate."""
    try:
        db = get_db()
        rows = db.execute('''
            SELECT page,
                   SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) errors,
                   COUNT(*) total
            FROM events_log
            WHERE event_type='request'
            AND created_at > datetime('now', '-6 hours')
            GROUP BY page
            HAVING total > 20
        ''').fetchall()
        db.close()
        for r in rows:
            rate = r['errors'] / r['total']
            if rate > 0.02:  # > 2%
                _insert_insight(
                    'reliability',
                    'critical' if rate > 0.10 else 'warning',
                    f"Erros em {r['page']}",
                    f"{r['errors']} erros de {r['total']} requests ({rate*100:.1f}%)",
                    rate * 100,
                    f"Investigar traceback em error_log; considerar rollback se deploy recente"
                )
    except Exception:
        pass


def analyze_feature_usage():
    """Identifica features subutilizadas."""
    try:
        db = get_db()
        # Features = paginas principais
        main_pages = ['/dashboard', '/traffic', '/ranking', '/marketplaces',
                      '/crm', '/logistica', '/pagamentos']
        rows = db.execute('''
            SELECT page, COUNT(DISTINCT user_id) users
            FROM events_log
            WHERE event_type='request'
            AND created_at > datetime('now', '-7 days')
            AND page IN ({})
            GROUP BY page
        '''.format(','.join('?' * len(main_pages))), main_pages).fetchall()
        db.close()

        if not rows:
            return
        max_users = max(r['users'] for r in rows) or 1
        for r in rows:
            usage_pct = r['users'] / max_users
            if usage_pct < 0.15:  # <15% do pico
                _insert_insight(
                    'engagement', 'info',
                    f"Feature subutilizada: {r['page']}",
                    f"Apenas {r['users']} usuarios unicos acessaram em 7 dias",
                    float(r['users']),
                    f"Melhorar visibilidade ou remover {r['page']} do menu principal"
                )
    except Exception:
        pass


def analyze_pricing_opportunities():
    """Detecta SKUs que perderam Buy Box (oportunidade pra pricing AI)."""
    try:
        db = get_db()
        rows = db.execute('''
            SELECT DISTINCT sku, marketplace
            FROM competitor_prices
            WHERE has_buybox = 1
            AND captured_at > datetime('now', '-1 hour')
            LIMIT 20
        ''').fetchall()
        db.close()
        if rows:
            _insert_insight(
                'revenue', 'opportunity',
                'SKUs com Buy Box de competidor detectados',
                f'{len(rows)} SKUs monitorados com competidor no Buy Box',
                float(len(rows)),
                'Ativar ai_pricing para recuperar Buy Box automaticamente'
            )
    except Exception:
        pass


def run_all():
    """
    Roda todas as analises.
    Chamar via cron a cada 6h (ou manualmente pelo admin).
    """
    ensure_tables()
    analyze_latency()
    analyze_errors()
    analyze_feature_usage()
    analyze_pricing_opportunities()

    try:
        from telemetry import track
        track('system', 'auto_insights_run')
    except Exception:
        pass
    return {'ok': True}


def get_recent_insights(limit=50, status='new'):
    """Retorna insights pendentes para o admin revisar."""
    db = get_db()
    sql = 'SELECT * FROM platform_insights WHERE 1=1'
    params = []
    if status:
        sql += ' AND status = ?'
        params.append(status)
    sql += ' ORDER BY created_at DESC LIMIT ?'
    params.append(limit)
    rows = [dict(r) for r in db.execute(sql, params).fetchall()]
    db.close()
    return rows


def mark_reviewed(insight_id, action='reviewed'):
    """Marca insight como visto/resolvido."""
    db = get_db()
    db.execute(
        'UPDATE platform_insights SET status=? WHERE id=?',
        (action, insight_id)
    )
    db.commit()
    db.close()
