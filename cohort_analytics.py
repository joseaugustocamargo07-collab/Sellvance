# cohort_analytics.py — Analise de coortes de clientes e retencao
# Calcula:
#   - Retencao por coorte (semana/mes de aquisicao)
#   - LTV por canal/marketplace
#   - Churn rate
#   - CAC vs LTV (payback period)

from database import get_db


def ensure_tables():
    """Bootstrap — usa dados existentes de contacts/orders."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS cohort_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            cohort_period   TEXT NOT NULL,
            period_type     TEXT DEFAULT 'month',
            size            INTEGER DEFAULT 0,
            retained_1      INTEGER DEFAULT 0,
            retained_2      INTEGER DEFAULT 0,
            retained_3      INTEGER DEFAULT 0,
            retained_6      INTEGER DEFAULT 0,
            ltv_current     REAL DEFAULT 0,
            captured_at     TEXT DEFAULT (datetime('now'))
        );
    ''')
    db.commit()
    db.close()


def get_monthly_cohorts(org_id, months_back=6):
    """
    Calcula retencao mensal para ultimos N meses.
    Uma coorte = clientes que fizeram 1o pedido naquele mes.
    Retencao = % que voltou a comprar nos meses seguintes.
    """
    db = get_db()
    try:
        # Coortes baseadas em first_order_at dos contatos
        cohorts_raw = db.execute('''
            SELECT strftime('%Y-%m', MIN(o.created_at)) as cohort,
                   c.id as contact_id
            FROM contacts c
            JOIN orders o ON o.contact_id = c.id
            WHERE c.org_id = ?
            GROUP BY c.id
            HAVING cohort >= strftime('%Y-%m', date('now', ?))
        ''', (org_id, f'-{months_back} months')).fetchall()

        cohort_map = {}
        for row in cohorts_raw:
            cohort_map.setdefault(row['cohort'], []).append(row['contact_id'])

        result = []
        for cohort, contact_ids in sorted(cohort_map.items()):
            if not contact_ids:
                continue
            placeholders = ','.join('?' * len(contact_ids))

            # Retencao M+1, M+2, M+3
            retention = {}
            for months_offset in [1, 2, 3, 6]:
                retained = db.execute(f'''
                    SELECT COUNT(DISTINCT contact_id) c
                    FROM orders
                    WHERE contact_id IN ({placeholders})
                    AND strftime('%Y-%m', created_at) = strftime('%Y-%m', date(?, '+{months_offset} months'))
                ''', (*contact_ids, f'{cohort}-01')).fetchone()
                retention[f'm{months_offset}'] = retained['c'] if retained else 0

            # LTV atual da coorte
            ltv = db.execute(f'''
                SELECT COALESCE(SUM(revenue), 0) s
                FROM orders
                WHERE contact_id IN ({placeholders})
            ''', tuple(contact_ids)).fetchone()
            ltv_total = ltv['s'] if ltv else 0
            avg_ltv = ltv_total / len(contact_ids) if contact_ids else 0

            result.append({
                'cohort': cohort,
                'size': len(contact_ids),
                'retained': retention,
                'retention_pct': {
                    k: round((v / len(contact_ids)) * 100, 1)
                    for k, v in retention.items()
                },
                'avg_ltv': round(avg_ltv, 2),
                'total_revenue': round(ltv_total, 2),
            })

        db.close()
        return result
    except Exception as e:
        db.close()
        return {'error': str(e)[:200]}


def get_ltv_by_channel(org_id):
    """LTV medio por canal de aquisicao."""
    db = get_db()
    try:
        rows = db.execute('''
            SELECT
                COALESCE(c.source, 'unknown') as channel,
                COUNT(DISTINCT c.id) as customers,
                COALESCE(SUM(o.revenue), 0) as total_revenue,
                COALESCE(AVG(o.revenue), 0) as avg_order_value
            FROM contacts c
            LEFT JOIN orders o ON o.contact_id = c.id
            WHERE c.org_id = ?
            GROUP BY c.source
            ORDER BY total_revenue DESC
        ''', (org_id,)).fetchall()
        db.close()

        result = []
        for r in rows:
            r = dict(r)
            r['avg_ltv'] = round(r['total_revenue'] / max(r['customers'], 1), 2)
            result.append(r)
        return result
    except Exception as e:
        db.close()
        return {'error': str(e)[:200]}


def get_churn_rate(org_id, days_inactive=90):
    """
    Taxa de churn = clientes sem compra nos ultimos N dias
    sobre total de clientes ativos.
    """
    db = get_db()
    try:
        total = db.execute(
            'SELECT COUNT(*) c FROM contacts WHERE org_id=?',
            (org_id,)
        ).fetchone()
        churned = db.execute('''
            SELECT COUNT(DISTINCT c.id) cnt
            FROM contacts c
            WHERE c.org_id = ?
            AND c.last_order_at IS NOT NULL
            AND c.last_order_at < datetime('now', ?)
        ''', (org_id, f'-{days_inactive} days')).fetchone()
        db.close()

        total_c = total['c'] if total else 0
        churned_c = churned['cnt'] if churned else 0
        return {
            'total_customers': total_c,
            'churned': churned_c,
            'churn_rate': round((churned_c / max(total_c, 1)) * 100, 1),
            'threshold_days': days_inactive,
        }
    except Exception as e:
        db.close()
        return {'error': str(e)[:200]}


def get_top_customers(org_id, limit=20):
    """Top clientes por LTV."""
    db = get_db()
    try:
        rows = db.execute('''
            SELECT
                c.id, c.name, c.email, c.phone, c.rfm_segment,
                COUNT(o.id) as order_count,
                COALESCE(SUM(o.revenue), 0) as ltv,
                MAX(o.created_at) as last_order
            FROM contacts c
            LEFT JOIN orders o ON o.contact_id = c.id
            WHERE c.org_id = ?
            GROUP BY c.id
            ORDER BY ltv DESC
            LIMIT ?
        ''', (org_id, limit)).fetchall()
        db.close()
        return [dict(r) for r in rows]
    except Exception as e:
        db.close()
        return {'error': str(e)[:200]}


def compute_full_report(org_id):
    """Relatorio completo de cohort analytics."""
    return {
        'monthly_cohorts': get_monthly_cohorts(org_id),
        'ltv_by_channel': get_ltv_by_channel(org_id),
        'churn': get_churn_rate(org_id),
        'top_customers': get_top_customers(org_id, limit=10),
    }
