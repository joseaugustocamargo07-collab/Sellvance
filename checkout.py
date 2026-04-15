# checkout.py — Sistema de checkout via Pix
# Gera cobranca Pix para conversao de trial → assinatura ativa.
#
# Arquitetura em 2 etapas:
#   1. MVP (agora): gera Pix Copia e Cola manual + QR code + instrucoes
#      - User escaneia QR / copia codigo / paga
#      - Admin confirma manualmente (endpoint /admin/checkout/confirm)
#      - Billing.activate_subscription roda
#
#   2. Produção (futuro): integracao com provedor (Efí, Gerencianet,
#      Mercado Pago, Asaas, etc) que gera Pix dinamico + webhook de
#      confirmacao automatica

import json
import time
import secrets
from database import get_db


def ensure_tables():
    """Bootstrap das tabelas de checkout."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS checkout_sessions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            user_id         INTEGER,
            plan            TEXT NOT NULL,
            amount          REAL NOT NULL,
            method          TEXT DEFAULT 'pix',
            status          TEXT DEFAULT 'pending',
            pix_code        TEXT,
            pix_qrcode_url  TEXT,
            external_ref    TEXT,
            expires_at      TEXT,
            paid_at         TEXT,
            confirmed_by    TEXT,
            notes           TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_checkout_status
            ON checkout_sessions(org_id, status, created_at);

        CREATE TABLE IF NOT EXISTS payments (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            checkout_id     INTEGER REFERENCES checkout_sessions(id),
            amount          REAL NOT NULL,
            method          TEXT,
            plan            TEXT,
            status          TEXT DEFAULT 'completed',
            external_ref    TEXT,
            paid_at         TEXT DEFAULT (datetime('now'))
        );
    ''')
    db.commit()
    db.close()


def create_session(org_id, user_id, plan, amount, method='pix'):
    """
    Cria uma nova sessao de checkout.
    Retorna dict com id da sessao, codigo Pix e QR code URL.
    """
    import urllib.parse as _up
    session_ref = secrets.token_urlsafe(16)

    # Pix Copia e Cola — gerado pela chave Pix da empresa
    # Por enquanto usamos um placeholder. Substituir por Pix dinamico quando
    # integracao com PSP estiver pronta.
    pix_key = 'pagamentos@sellvance.com.br'  # chave Pix da empresa
    pix_code = f'00020126580014BR.GOV.BCB.PIX0136{pix_key}0210SLV{session_ref[:8]}52040000530398654{len(str(int(amount*100))):02d}{int(amount*100)}5802BR5910SELLVANCE6008SAOPAULO62070503***6304ABCD'

    # QR Code usando API publica
    qr_data = _up.quote(pix_code)
    pix_qrcode_url = f'https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={qr_data}'

    db = get_db()
    cur = db.execute(
        '''INSERT INTO checkout_sessions
           (org_id, user_id, plan, amount, method, status,
            pix_code, pix_qrcode_url, external_ref, expires_at)
           VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?,
                   datetime('now', '+30 minutes'))''',
        (org_id, user_id, plan, amount, method, pix_code, pix_qrcode_url, session_ref)
    )
    session_id = cur.lastrowid
    db.commit()
    db.close()

    try:
        from telemetry import track
        track('action', 'checkout_session_created', plan=plan, amount=amount)
    except Exception:
        pass

    return {
        'session_id': session_id,
        'external_ref': session_ref,
        'pix_code': pix_code,
        'pix_qrcode_url': pix_qrcode_url,
        'amount': amount,
        'plan': plan,
        'expires_in_minutes': 30,
    }


def get_session(session_id, org_id=None):
    """Retorna detalhes de uma sessao de checkout."""
    db = get_db()
    sql = 'SELECT * FROM checkout_sessions WHERE id=?'
    params = [session_id]
    if org_id is not None:
        sql += ' AND org_id=?'
        params.append(org_id)
    row = db.execute(sql, params).fetchone()
    db.close()
    return dict(row) if row else None


def confirm_payment(session_id, confirmed_by='admin', notes=''):
    """
    Confirma o pagamento de uma sessao (admin manual ou webhook).
    Dispara activate_subscription do billing.
    """
    db = get_db()
    row = db.execute(
        'SELECT * FROM checkout_sessions WHERE id=?', (session_id,)
    ).fetchone()
    if not row:
        db.close()
        return {'ok': False, 'error': 'session nao encontrada'}

    s = dict(row)
    if s['status'] == 'paid':
        db.close()
        return {'ok': False, 'error': 'ja confirmado'}

    db.execute(
        '''UPDATE checkout_sessions
           SET status='paid', paid_at=datetime('now'),
               confirmed_by=?, notes=?
           WHERE id=?''',
        (confirmed_by, notes, session_id)
    )
    db.execute(
        '''INSERT INTO payments
           (org_id, checkout_id, amount, method, plan, external_ref)
           VALUES (?, ?, ?, ?, ?, ?)''',
        (s['org_id'], session_id, s['amount'], s['method'],
         s['plan'], s['external_ref'])
    )
    db.commit()
    db.close()

    # Ativa subscription
    try:
        import billing
        billing.activate_subscription(s['org_id'], payment_method=s['method'])
    except Exception as e:
        print(f'[checkout] billing activation error: {e}')

    try:
        from telemetry import track
        track('action', 'checkout_paid', plan=s['plan'], amount=s['amount'])
    except Exception:
        pass

    return {'ok': True, 'session': s}


def cancel_session(session_id, reason=''):
    db = get_db()
    db.execute(
        '''UPDATE checkout_sessions
           SET status='cancelled', notes=?
           WHERE id=? AND status='pending' ''',
        (reason, session_id)
    )
    db.commit()
    db.close()
    return True


def expire_old_sessions():
    """Marca sessoes antigas como expiradas (cron)."""
    db = get_db()
    db.execute(
        '''UPDATE checkout_sessions
           SET status='expired'
           WHERE status='pending'
           AND expires_at < datetime('now')'''
    )
    affected = db.total_changes
    db.commit()
    db.close()
    return affected


def get_pending_for_review(org_id=None, limit=50):
    """Lista sessoes pendentes pro admin revisar/confirmar."""
    db = get_db()
    sql = '''SELECT * FROM checkout_sessions
             WHERE status='pending' AND expires_at > datetime('now')'''
    params = []
    if org_id is not None:
        sql += ' AND org_id=?'
        params.append(org_id)
    sql += ' ORDER BY created_at DESC LIMIT ?'
    params.append(limit)
    rows = [dict(r) for r in db.execute(sql, params).fetchall()]
    db.close()
    return rows


def get_stats(org_id=None):
    """Estatisticas de pagamentos."""
    db = get_db()
    where = ''
    params = []
    if org_id is not None:
        where = 'WHERE org_id=?'
        params = [org_id]

    total_revenue = db.execute(
        f'SELECT COALESCE(SUM(amount), 0) s FROM payments {where}', params
    ).fetchone()
    sessions_pending = db.execute(
        f'SELECT COUNT(*) c FROM checkout_sessions {where} {"AND" if where else "WHERE"} status=\'pending\'',
        params
    ).fetchone()
    sessions_paid = db.execute(
        f'SELECT COUNT(*) c FROM checkout_sessions {where} {"AND" if where else "WHERE"} status=\'paid\'',
        params
    ).fetchone()
    db.close()

    return {
        'total_revenue': round(total_revenue['s'] if total_revenue else 0, 2),
        'sessions_pending': sessions_pending['c'] if sessions_pending else 0,
        'sessions_paid': sessions_paid['c'] if sessions_paid else 0,
    }
