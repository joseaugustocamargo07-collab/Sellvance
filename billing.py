# billing.py — Gestao de trial e assinaturas
# Sistema de trial de 14 dias + status de assinatura.
# Para o MVP usamos apenas tracking. Integracao com Stripe/Pix vem depois.

from database import get_db


# Planos disponiveis (precos em BRL)
PLAN_PRICING = {
    'marketplaces': {'price': 197, 'label': 'Marketplaces', 'trial_days': 14},
    'marketing':    {'price': 247, 'label': 'Marketing',    'trial_days': 14},
    'completo':     {'price': 397, 'label': 'Completo',     'trial_days': 14},
}


def ensure_tables():
    """Bootstrap das tabelas de billing."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER UNIQUE NOT NULL,
            plan            TEXT NOT NULL,
            status          TEXT DEFAULT 'trialing',
            trial_started   TEXT DEFAULT (datetime('now')),
            trial_ends      TEXT,
            subscribed_at   TEXT,
            billing_email   TEXT,
            next_billing    TEXT,
            last_payment    TEXT,
            payment_method  TEXT DEFAULT 'none',
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS billing_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            event_type      TEXT NOT NULL,
            amount          REAL,
            plan            TEXT,
            notes           TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );
    ''')
    db.commit()
    db.close()


def start_trial(org_id, plan='completo', billing_email=None):
    """Inicia o trial de 14 dias."""
    db = get_db()
    existing = db.execute(
        'SELECT id FROM subscriptions WHERE org_id=?',
        (org_id,)
    ).fetchone()
    if existing:
        db.close()
        return {'ok': False, 'error': 'subscription already exists'}

    trial_days = PLAN_PRICING.get(plan, {}).get('trial_days', 14)
    db.execute(
        '''INSERT INTO subscriptions
           (org_id, plan, status, trial_ends, billing_email)
           VALUES (?, ?, 'trialing', datetime('now', ?), ?)''',
        (org_id, plan, f'+{trial_days} days', billing_email)
    )
    db.execute(
        '''INSERT INTO billing_events (org_id, event_type, plan, notes)
           VALUES (?, 'trial_started', ?, ?)''',
        (org_id, plan, f'Trial de {trial_days} dias iniciado')
    )
    db.commit()
    db.close()

    try:
        from telemetry import track
        track('action', 'trial_started', plan=plan, days=trial_days)
    except Exception:
        pass
    return {'ok': True, 'trial_days': trial_days}


def get_subscription(org_id):
    """Retorna subscription da org."""
    db = get_db()
    row = db.execute(
        'SELECT * FROM subscriptions WHERE org_id=?',
        (org_id,)
    ).fetchone()
    db.close()
    return dict(row) if row else None


def get_trial_status(org_id):
    """
    Retorna status do trial/assinatura.
    {
      'active': bool,
      'status': 'trialing' | 'active' | 'expired' | 'canceled',
      'days_left': int,
      'plan': str,
      'trial_ends': iso,
      'price': int
    }
    """
    sub = get_subscription(org_id)
    if not sub:
        return {'active': False, 'status': 'none', 'days_left': 0}

    db = get_db()
    days_row = db.execute(
        '''SELECT CAST(julianday(trial_ends) - julianday('now') AS INTEGER) as days
           FROM subscriptions WHERE org_id=?''',
        (org_id,)
    ).fetchone()
    db.close()
    days_left = max(0, days_row['days']) if days_row and days_row['days'] is not None else 0

    status = sub['status']
    # Auto-expira
    if status == 'trialing' and days_left <= 0:
        status = 'expired'
        db = get_db()
        db.execute("UPDATE subscriptions SET status='expired' WHERE org_id=?", (org_id,))
        db.commit()
        db.close()

    return {
        'active': status in ('trialing', 'active'),
        'status': status,
        'days_left': days_left,
        'plan': sub['plan'],
        'trial_ends': sub['trial_ends'],
        'price': PLAN_PRICING.get(sub['plan'], {}).get('price', 0),
        'label': PLAN_PRICING.get(sub['plan'], {}).get('label', sub['plan']),
    }


def activate_subscription(org_id, payment_method='pix'):
    """Marca subscription como ativa (apos pagamento)."""
    db = get_db()
    db.execute(
        '''UPDATE subscriptions
           SET status='active', subscribed_at=datetime('now'),
               last_payment=datetime('now'),
               next_billing=datetime('now', '+30 days'),
               payment_method=?, updated_at=datetime('now')
           WHERE org_id=?''',
        (payment_method, org_id)
    )
    sub = db.execute('SELECT plan FROM subscriptions WHERE org_id=?', (org_id,)).fetchone()
    plan = sub['plan'] if sub else 'completo'
    price = PLAN_PRICING.get(plan, {}).get('price', 0)
    db.execute(
        '''INSERT INTO billing_events (org_id, event_type, amount, plan, notes)
           VALUES (?, 'subscription_activated', ?, ?, ?)''',
        (org_id, price, plan, f'Pagamento via {payment_method}')
    )
    db.commit()
    db.close()
    return True


def cancel_subscription(org_id, reason=''):
    db = get_db()
    db.execute(
        '''UPDATE subscriptions
           SET status='canceled', updated_at=datetime('now')
           WHERE org_id=?''',
        (org_id,)
    )
    db.execute(
        '''INSERT INTO billing_events (org_id, event_type, notes)
           VALUES (?, 'canceled', ?)''',
        (org_id, reason)
    )
    db.commit()
    db.close()
    return True


def get_onboarding_status(org_id):
    """
    Checa quais steps do onboarding foram concluidos.
    Retorna dict com bool por step.
    """
    db = get_db()
    # Step 1: tem marketplace conectado
    mp = db.execute(
        '''SELECT COUNT(*) c FROM api_integrations
           WHERE org_id=? AND status='connected' ''',
        (org_id,)
    ).fetchone()

    # Step 2: tem nome de org setado (diferente do default)
    org = db.execute(
        "SELECT name FROM organizations WHERE id=?", (org_id,)
    ).fetchone()

    # Step 3: tem regra de pricing
    pr = db.execute(
        '''SELECT COUNT(*) c FROM pricing_rules WHERE org_id=?''',
        (org_id,)
    ).fetchone()
    db.close()

    # Step 4: whatsapp agent habilitado
    try:
        from feature_flags import is_enabled
        wa = is_enabled('whatsapp_agent', org_id)
    except Exception:
        wa = False

    steps = {
        'marketplace': (mp['c'] if mp else 0) > 0,
        'org_info': bool(org and org['name'] and org['name'] != 'Sellvance'),
        'pricing_rule': (pr['c'] if pr else 0) > 0,
        'whatsapp': wa,
    }
    completed = sum(1 for v in steps.values() if v)
    return {
        'steps': steps,
        'completed': completed,
        'total': 4,
        'progress_pct': int((completed / 4) * 100),
        'active_step': next(
            (i+1 for i, (_, v) in enumerate(steps.items()) if not v),
            4
        ),
    }
