# asaas_api.py — Integracao com Asaas (PSP brasileiro)
# Cria clientes, cobranças Pix/Boleto/Cartão, assinaturas recorrentes
# e recebe webhooks de confirmação automatica de pagamento.
#
# Docs: https://docs.asaas.com/docs
# API Base: https://api.asaas.com/v3 (produção)
#           https://sandbox.asaas.com/api/v3 (sandbox)
#
# Env vars:
#   ASAAS_API_KEY     — sua API key (pega em Configurações → Integrações no Asaas)
#   ASAAS_ENV         — 'production' ou 'sandbox' (default: sandbox)
#   ASAAS_WEBHOOK_TOKEN — token para validar webhooks (você define, qualquer string)

import os
import json
import urllib.request
import urllib.parse
from database import get_db


def _get_api_key():
    return (os.environ.get('ASAAS_API_KEY', '') or '').strip()


def _get_api_base():
    env = (os.environ.get('ASAAS_ENV', 'sandbox') or '').strip().lower()
    if env == 'production':
        return 'https://api.asaas.com/v3'
    return 'https://sandbox.asaas.com/api/v3'


def _get_webhook_token():
    return (os.environ.get('ASAAS_WEBHOOK_TOKEN', 'sellvance_asaas_hook_2026') or '').strip()


def is_configured():
    return bool(_get_api_key())


def ensure_tables():
    """Bootstrap das tabelas Asaas no banco local."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS asaas_customers (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            asaas_id        TEXT UNIQUE,
            name            TEXT,
            email           TEXT,
            cpf_cnpj        TEXT,
            phone           TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS asaas_subscriptions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            asaas_sub_id    TEXT UNIQUE,
            asaas_customer_id TEXT,
            plan            TEXT,
            value           REAL,
            cycle           TEXT DEFAULT 'MONTHLY',
            billing_type    TEXT DEFAULT 'PIX',
            status          TEXT DEFAULT 'ACTIVE',
            next_due_date   TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS asaas_payments (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            asaas_payment_id TEXT UNIQUE,
            asaas_sub_id    TEXT,
            billing_type    TEXT,
            value           REAL,
            status          TEXT DEFAULT 'PENDING',
            pix_payload     TEXT,
            pix_qrcode_b64  TEXT,
            pix_expiration  TEXT,
            due_date        TEXT,
            paid_at         TEXT,
            invoice_url     TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS asaas_webhook_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event           TEXT,
            payment_id      TEXT,
            status          TEXT,
            raw_payload     TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );
    ''')
    db.commit()
    db.close()


def _api(method, path, data=None):
    """Chamada generica pra Asaas API."""
    api_key = _get_api_key()
    if not api_key:
        return {'error': 'ASAAS_API_KEY nao configurado'}

    url = f'{_get_api_base()}{path}'
    headers = {
        'access_token': api_key,
        'Content-Type': 'application/json',
        'User-Agent': 'Sellvance/1.0',
    }

    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        err = e.read().decode('utf-8', errors='replace')[:1000]
        try:
            return json.loads(err)
        except Exception:
            return {'error': e.code, 'message': err[:500]}
    except Exception as e:
        return {'error': str(e)[:300]}


# ══════════════════════════════════════════════════════════════════════════
#  CUSTOMERS (Clientes)
# ══════════════════════════════════════════════════════════════════════════

def create_customer(org_id, name, email, cpf_cnpj=None, phone=None):
    """Cria cliente no Asaas (necessario antes de cobrar)."""
    # Verifica se ja existe
    db = get_db()
    existing = db.execute(
        'SELECT asaas_id FROM asaas_customers WHERE org_id=?',
        (org_id,)
    ).fetchone()
    if existing and existing['asaas_id']:
        db.close()
        return {'ok': True, 'customer_id': existing['asaas_id'], 'already_existed': True}
    db.close()

    payload = {
        'name': name,
        'email': email,
    }
    if cpf_cnpj:
        payload['cpfCnpj'] = cpf_cnpj.replace('.', '').replace('-', '').replace('/', '')
    if phone:
        payload['phone'] = phone.replace('(', '').replace(')', '').replace('-', '').replace(' ', '')

    result = _api('POST', '/customers', payload)
    if result.get('id'):
        db = get_db()
        db.execute(
            '''INSERT OR REPLACE INTO asaas_customers
               (org_id, asaas_id, name, email, cpf_cnpj, phone)
               VALUES (?, ?, ?, ?, ?, ?)''',
            (org_id, result['id'], name, email, cpf_cnpj, phone)
        )
        db.commit()
        db.close()
        return {'ok': True, 'customer_id': result['id']}
    return {'ok': False, 'error': result}


def get_or_create_customer(org_id):
    """Pega customer_id existente ou cria um novo baseado nos dados do org."""
    db = get_db()
    existing = db.execute(
        'SELECT asaas_id FROM asaas_customers WHERE org_id=?',
        (org_id,)
    ).fetchone()
    if existing and existing['asaas_id']:
        db.close()
        return existing['asaas_id']

    # Buscar dados do usuario
    user = db.execute(
        'SELECT name, email FROM users WHERE org_id=? LIMIT 1',
        (org_id,)
    ).fetchone()
    org = db.execute(
        'SELECT name FROM organizations WHERE id=?',
        (org_id,)
    ).fetchone()
    db.close()

    if not user:
        return None

    result = create_customer(
        org_id,
        name=user['name'],
        email=user['email'],
    )
    return result.get('customer_id') if result.get('ok') else None


# ══════════════════════════════════════════════════════════════════════════
#  COBRANCAS PIX (Pagamento avulso)
# ══════════════════════════════════════════════════════════════════════════

def create_pix_charge(org_id, amount, description='Assinatura Sellvance', due_date=None):
    """
    Cria uma cobranca via Pix no Asaas.
    Retorna o QR code e codigo copia-e-cola.
    """
    customer_id = get_or_create_customer(org_id)
    if not customer_id:
        return {'ok': False, 'error': 'nao foi possivel criar/buscar customer no Asaas'}

    if not due_date:
        from datetime import datetime, timedelta
        due_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

    payload = {
        'customer': customer_id,
        'billingType': 'PIX',
        'value': float(amount),
        'dueDate': due_date,
        'description': description,
    }
    result = _api('POST', '/payments', payload)
    if not result.get('id'):
        return {'ok': False, 'error': result}

    payment_id = result['id']

    # Buscar QR code
    qr_result = _api('GET', f'/payments/{payment_id}/pixQrCode')
    pix_payload = qr_result.get('payload', '')
    pix_qrcode_b64 = qr_result.get('encodedImage', '')
    pix_expiration = qr_result.get('expirationDate', '')

    # Salvar no banco local
    db = get_db()
    db.execute(
        '''INSERT INTO asaas_payments
           (org_id, asaas_payment_id, billing_type, value, status,
            pix_payload, pix_qrcode_b64, pix_expiration, due_date, invoice_url)
           VALUES (?, ?, 'PIX', ?, 'PENDING', ?, ?, ?, ?, ?)''',
        (org_id, payment_id, amount, pix_payload, pix_qrcode_b64,
         pix_expiration, due_date, result.get('invoiceUrl'))
    )
    db.commit()
    db.close()

    try:
        from telemetry import track
        track('action', 'asaas_pix_created', amount=amount)
    except Exception:
        pass

    return {
        'ok': True,
        'payment_id': payment_id,
        'pix_payload': pix_payload,       # Copia e cola
        'pix_qrcode_b64': pix_qrcode_b64, # QR code base64 pra <img src="data:image/png;base64,XXX">
        'pix_expiration': pix_expiration,
        'amount': amount,
        'invoice_url': result.get('invoiceUrl'),
        'status': result.get('status', 'PENDING'),
    }


# ══════════════════════════════════════════════════════════════════════════
#  ASSINATURAS RECORRENTES (Subscription)
# ══════════════════════════════════════════════════════════════════════════

def create_subscription(org_id, plan, amount, billing_type='PIX', cycle='MONTHLY'):
    """
    Cria assinatura recorrente no Asaas.
    Asaas gera automaticamente as cobranças mês a mês.
    """
    customer_id = get_or_create_customer(org_id)
    if not customer_id:
        return {'ok': False, 'error': 'nao foi possivel criar/buscar customer'}

    from datetime import datetime, timedelta
    next_due = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

    payload = {
        'customer': customer_id,
        'billingType': billing_type,
        'value': float(amount),
        'nextDueDate': next_due,
        'cycle': cycle,
        'description': f'Sellvance Plano {plan.title()} — Mensal',
    }
    result = _api('POST', '/subscriptions', payload)
    if not result.get('id'):
        return {'ok': False, 'error': result}

    db = get_db()
    db.execute(
        '''INSERT INTO asaas_subscriptions
           (org_id, asaas_sub_id, asaas_customer_id, plan, value,
            cycle, billing_type, status, next_due_date)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)''',
        (org_id, result['id'], customer_id, plan, amount,
         cycle, billing_type, next_due)
    )
    db.commit()
    db.close()

    try:
        from telemetry import track
        track('action', 'asaas_subscription_created', plan=plan, amount=amount)
    except Exception:
        pass

    return {
        'ok': True,
        'subscription_id': result['id'],
        'plan': plan,
        'amount': amount,
        'cycle': cycle,
        'next_due_date': next_due,
        'status': 'ACTIVE',
    }


def cancel_subscription(org_id):
    """Cancela assinatura ativa."""
    db = get_db()
    row = db.execute(
        "SELECT asaas_sub_id FROM asaas_subscriptions WHERE org_id=? AND status='ACTIVE' ORDER BY id DESC LIMIT 1",
        (org_id,)
    ).fetchone()
    db.close()
    if not row:
        return {'ok': False, 'error': 'nenhuma assinatura ativa'}

    result = _api('DELETE', f"/subscriptions/{row['asaas_sub_id']}")

    db = get_db()
    db.execute(
        "UPDATE asaas_subscriptions SET status='CANCELLED' WHERE asaas_sub_id=?",
        (row['asaas_sub_id'],)
    )
    db.commit()
    db.close()

    try:
        import billing
        billing.cancel_subscription(org_id, reason='asaas_cancelled')
    except Exception:
        pass

    return {'ok': True, 'cancelled': row['asaas_sub_id']}


# ══════════════════════════════════════════════════════════════════════════
#  WEBHOOKS (Confirmacao automatica)
# ══════════════════════════════════════════════════════════════════════════

def process_webhook(event_data):
    """
    Processa webhook do Asaas.
    Chamado pelo endpoint /api/asaas/webhook (POST).

    Eventos importantes:
      - PAYMENT_RECEIVED  → pagamento confirmado
      - PAYMENT_CONFIRMED → pagamento confirmado (cartao)
      - PAYMENT_OVERDUE   → cobranca vencida
      - PAYMENT_DELETED   → cobranca cancelada

    Retorna True se processou algo relevante.
    """
    event = event_data.get('event', '')
    payment = event_data.get('payment', {})
    payment_id = payment.get('id', '')

    # Log
    db = get_db()
    db.execute(
        '''INSERT INTO asaas_webhook_log (event, payment_id, status, raw_payload)
           VALUES (?, ?, ?, ?)''',
        (event, payment_id, payment.get('status', ''),
         json.dumps(event_data)[:5000])
    )
    db.commit()
    db.close()

    if event in ('PAYMENT_RECEIVED', 'PAYMENT_CONFIRMED'):
        return _handle_payment_confirmed(payment_id, payment)
    elif event == 'PAYMENT_OVERDUE':
        _update_local_payment_status(payment_id, 'OVERDUE')
        return True

    return False


def _handle_payment_confirmed(payment_id, payment_data):
    """Ativa a subscription quando pagamento e confirmado."""
    db = get_db()
    # Atualiza status do pagamento local
    db.execute(
        '''UPDATE asaas_payments
           SET status='RECEIVED', paid_at=datetime('now')
           WHERE asaas_payment_id=?''',
        (payment_id,)
    )

    # Buscar org_id pelo payment_id local
    row = db.execute(
        'SELECT org_id FROM asaas_payments WHERE asaas_payment_id=?',
        (payment_id,)
    ).fetchone()
    db.commit()
    db.close()

    if row:
        org_id = row['org_id']
        # Ativar subscription via billing
        try:
            import billing
            billing.activate_subscription(org_id, payment_method='pix_asaas')
        except Exception as e:
            print(f'[asaas] billing activation error: {e}')

        try:
            from telemetry import track
            track('action', 'asaas_payment_received', org_id=org_id,
                  payment_id=payment_id)
        except Exception:
            pass

        # Confirmar checkout_session se existir
        try:
            from checkout import get_pending_for_review, confirm_payment
            pending = get_pending_for_review(org_id=org_id)
            for p in pending:
                confirm_payment(p['id'], confirmed_by='asaas_webhook',
                                notes=f'Asaas payment {payment_id}')
        except Exception:
            pass

        return True
    return False


def _update_local_payment_status(payment_id, status):
    try:
        db = get_db()
        db.execute(
            'UPDATE asaas_payments SET status=? WHERE asaas_payment_id=?',
            (status, payment_id)
        )
        db.commit()
        db.close()
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════
#  STATUS e DIAGNOSTICO
# ══════════════════════════════════════════════════════════════════════════

def get_subscription_status(org_id):
    """Retorna status da assinatura Asaas de um org."""
    db = get_db()
    sub = db.execute(
        "SELECT * FROM asaas_subscriptions WHERE org_id=? ORDER BY id DESC LIMIT 1",
        (org_id,)
    ).fetchone()
    last_payment = db.execute(
        "SELECT * FROM asaas_payments WHERE org_id=? ORDER BY id DESC LIMIT 1",
        (org_id,)
    ).fetchone()
    db.close()
    return {
        'subscription': dict(sub) if sub else None,
        'last_payment': dict(last_payment) if last_payment else None,
    }


def get_admin_stats():
    """Estatisticas gerais de pagamentos Asaas."""
    db = get_db()
    total = db.execute(
        "SELECT COALESCE(SUM(value), 0) s, COUNT(*) c FROM asaas_payments WHERE status='RECEIVED'"
    ).fetchone()
    pending = db.execute(
        "SELECT COUNT(*) c FROM asaas_payments WHERE status='PENDING'"
    ).fetchone()
    active_subs = db.execute(
        "SELECT COUNT(*) c FROM asaas_subscriptions WHERE status='ACTIVE'"
    ).fetchone()
    db.close()
    return {
        'total_received': round(total['s'] if total else 0, 2),
        'payments_received': total['c'] if total else 0,
        'payments_pending': pending['c'] if pending else 0,
        'active_subscriptions': active_subs['c'] if active_subs else 0,
    }
