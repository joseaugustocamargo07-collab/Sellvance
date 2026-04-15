# whatsapp_api.py — Integracao real com Meta WhatsApp Business Cloud API
# Gerencia credenciais, envia mensagens e recebe webhooks.
#
# Credenciais necessarias:
#   - phone_number_id (da Meta Business Suite)
#   - access_token (System User token, NAO expira)
#   - app_secret (para validar webhook signature)
#   - verify_token (string qualquer definida por voce)

import json
import urllib.request
import urllib.parse
import hashlib
import hmac
from database import get_db


GRAPH_API_VERSION = 'v19.0'
GRAPH_API_BASE = f'https://graph.facebook.com/{GRAPH_API_VERSION}'


def ensure_tables():
    """Bootstrap da tabela de credenciais WhatsApp."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS whatsapp_credentials (
            org_id          INTEGER PRIMARY KEY,
            phone_number_id TEXT,
            access_token    TEXT,
            app_secret      TEXT,
            verify_token    TEXT,
            display_phone   TEXT,
            business_name   TEXT,
            status          TEXT DEFAULT 'pending',
            last_error      TEXT,
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS whatsapp_message_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            direction       TEXT NOT NULL,
            to_phone        TEXT,
            from_phone      TEXT,
            message_id      TEXT,
            content         TEXT,
            status          TEXT DEFAULT 'sent',
            error           TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );
    ''')
    db.commit()
    db.close()


def save_credentials(org_id, phone_number_id, access_token,
                     app_secret=None, verify_token=None,
                     display_phone=None, business_name=None):
    """Salva credenciais e testa conectividade com a Meta."""
    db = get_db()
    db.execute(
        '''INSERT OR REPLACE INTO whatsapp_credentials
           (org_id, phone_number_id, access_token, app_secret,
            verify_token, display_phone, business_name, status, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', datetime('now'))''',
        (org_id, phone_number_id, access_token, app_secret,
         verify_token, display_phone, business_name)
    )
    db.commit()
    db.close()

    # Testa conectividade
    test = test_connection(org_id)
    return test


def get_credentials(org_id):
    """Recupera credenciais da org."""
    db = get_db()
    row = db.execute(
        'SELECT * FROM whatsapp_credentials WHERE org_id=?',
        (org_id,)
    ).fetchone()
    db.close()
    return dict(row) if row else None


def test_connection(org_id):
    """Testa se as credenciais funcionam fazendo GET no phone_number_id."""
    creds = get_credentials(org_id)
    if not creds or not creds.get('access_token') or not creds.get('phone_number_id'):
        return {'ok': False, 'error': 'Credenciais nao configuradas'}

    url = f"{GRAPH_API_BASE}/{creds['phone_number_id']}?fields=display_phone_number,verified_name"
    req = urllib.request.Request(
        url,
        headers={'Authorization': f"Bearer {creds['access_token']}"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        db = get_db()
        db.execute(
            '''UPDATE whatsapp_credentials
               SET status='connected', display_phone=?, business_name=?,
                   last_error=NULL, updated_at=datetime('now')
               WHERE org_id=?''',
            (data.get('display_phone_number'), data.get('verified_name'), org_id)
        )
        db.commit()
        db.close()
        return {'ok': True, 'data': data}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode('utf-8', errors='replace')
        db = get_db()
        db.execute(
            "UPDATE whatsapp_credentials SET status='error', last_error=? WHERE org_id=?",
            (err_body[:500], org_id)
        )
        db.commit()
        db.close()
        return {'ok': False, 'error': f'HTTP {e.code}: {err_body[:200]}'}
    except Exception as e:
        return {'ok': False, 'error': str(e)[:200]}


def send_message(org_id, to_phone, text):
    """
    Envia mensagem de texto via WhatsApp Cloud API.
    to_phone: numero com codigo do pais sem +, ex: 5511999998888
    """
    creds = get_credentials(org_id)
    if not creds or creds.get('status') != 'connected':
        return {'ok': False, 'error': 'WhatsApp nao conectado'}

    phone = ''.join(c for c in str(to_phone) if c.isdigit())
    if len(phone) < 11:
        return {'ok': False, 'error': 'Telefone invalido'}

    url = f"{GRAPH_API_BASE}/{creds['phone_number_id']}/messages"
    payload = {
        'messaging_product': 'whatsapp',
        'to': phone,
        'type': 'text',
        'text': {'body': text[:4096]},  # limite da Meta
    }
    data_bytes = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        url,
        data=data_bytes,
        headers={
            'Authorization': f"Bearer {creds['access_token']}",
            'Content-Type': 'application/json',
        },
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())

        msg_id = (result.get('messages') or [{}])[0].get('id')
        db = get_db()
        db.execute(
            '''INSERT INTO whatsapp_message_log
               (org_id, direction, to_phone, message_id, content, status)
               VALUES (?, 'out', ?, ?, ?, 'sent')''',
            (org_id, phone, msg_id, text[:1000])
        )
        db.commit()
        db.close()

        try:
            from telemetry import track
            track('action', 'wa_msg_sent', to=phone[:5] + '***')
        except Exception:
            pass
        return {'ok': True, 'message_id': msg_id}
    except urllib.error.HTTPError as e:
        err = e.read().decode('utf-8', errors='replace')[:500]
        db = get_db()
        db.execute(
            '''INSERT INTO whatsapp_message_log
               (org_id, direction, to_phone, content, status, error)
               VALUES (?, 'out', ?, ?, 'failed', ?)''',
            (org_id, phone, text[:1000], err)
        )
        db.commit()
        db.close()
        return {'ok': False, 'error': f'HTTP {e.code}: {err[:200]}'}
    except Exception as e:
        return {'ok': False, 'error': str(e)[:200]}


def verify_webhook_signature(app_secret, signature_header, body_bytes):
    """
    Valida assinatura do webhook da Meta (X-Hub-Signature-256).
    """
    if not signature_header or not app_secret:
        return False
    if not signature_header.startswith('sha256='):
        return False
    expected = hmac.new(
        app_secret.encode('utf-8'),
        body_bytes,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature_header[7:], expected)


def parse_webhook_message(body_json):
    """
    Extrai mensagem de um payload do webhook da Meta.
    Retorna None se nao for mensagem, ou dict com {from, name, text, phone_number_id}.
    """
    try:
        entry = (body_json.get('entry') or [{}])[0]
        change = (entry.get('changes') or [{}])[0]
        value = change.get('value') or {}
        messages = value.get('messages') or []
        if not messages:
            return None
        msg = messages[0]
        contacts = value.get('contacts') or [{}]
        contact = contacts[0]

        text_body = ''
        if msg.get('type') == 'text':
            text_body = (msg.get('text') or {}).get('body', '')
        elif msg.get('type') == 'button':
            text_body = (msg.get('button') or {}).get('text', '')
        elif msg.get('type') == 'interactive':
            interactive = msg.get('interactive') or {}
            text_body = (interactive.get('button_reply') or interactive.get('list_reply') or {}).get('title', '')

        return {
            'from': msg.get('from'),
            'name': (contact.get('profile') or {}).get('name', ''),
            'text': text_body,
            'message_id': msg.get('id'),
            'phone_number_id': (value.get('metadata') or {}).get('phone_number_id'),
        }
    except Exception:
        return None


def find_org_by_phone_number_id(phone_number_id):
    """Resolve qual org uma mensagem pertence pelo phone_number_id."""
    if not phone_number_id:
        return None
    db = get_db()
    row = db.execute(
        'SELECT org_id FROM whatsapp_credentials WHERE phone_number_id=?',
        (phone_number_id,)
    ).fetchone()
    db.close()
    return row['org_id'] if row else None
