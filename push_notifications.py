# push_notifications.py — Web Push Notifications via VAPID
# Armazena subscriptions do service worker e envia notificacoes.
#
# Para producao, instalar pywebpush:
#   pip install pywebpush cryptography
# Se nao estiver instalado, o modulo segue funcionando em modo "no-op"
# (grava logs mas nao envia de verdade).

import json
from database import get_db


try:
    from pywebpush import webpush, WebPushException
    PYWEBPUSH_AVAILABLE = True
except ImportError:
    PYWEBPUSH_AVAILABLE = False


def ensure_tables():
    """Bootstrap das tabelas de push notifications."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS push_subscriptions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            user_id         INTEGER,
            endpoint        TEXT UNIQUE NOT NULL,
            p256dh_key      TEXT NOT NULL,
            auth_key        TEXT NOT NULL,
            user_agent      TEXT,
            is_active       INTEGER DEFAULT 1,
            created_at      TEXT DEFAULT (datetime('now')),
            last_sent_at    TEXT
        );

        CREATE TABLE IF NOT EXISTS vapid_config (
            id              INTEGER PRIMARY KEY CHECK (id = 1),
            public_key      TEXT NOT NULL,
            private_key     TEXT NOT NULL,
            subject         TEXT DEFAULT 'mailto:admin@sellvance.com.br',
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS push_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER,
            subscription_id INTEGER,
            title           TEXT,
            body            TEXT,
            status          TEXT,
            error           TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );
    ''')
    db.commit()
    db.close()


def generate_vapid_keys():
    """
    Gera um par de chaves VAPID (ECDSA P-256).
    Retorna dict {public_key, private_key} ambos em base64url.
    Requer cryptography.
    """
    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import ec
        import base64

        private_key = ec.generate_private_key(ec.SECP256R1())
        private_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.X962,
            format=serialization.PublicFormat.UncompressedPoint
        )
        public_b64 = base64.urlsafe_b64encode(public_bytes).decode('ascii').rstrip('=')

        return {
            'public_key': public_b64,
            'private_key_pem': private_bytes.decode('ascii'),
        }
    except ImportError:
        return None


def get_or_create_vapid_keys():
    """Busca as chaves VAPID ou gera um novo par."""
    db = get_db()
    row = db.execute('SELECT * FROM vapid_config WHERE id=1').fetchone()
    if row:
        db.close()
        return dict(row)

    db.close()
    keys = generate_vapid_keys()
    if not keys:
        return None

    db = get_db()
    db.execute(
        '''INSERT INTO vapid_config (id, public_key, private_key, subject)
           VALUES (1, ?, ?, ?)''',
        (keys['public_key'], keys['private_key_pem'], 'mailto:admin@sellvance.com.br')
    )
    db.commit()
    row = db.execute('SELECT * FROM vapid_config WHERE id=1').fetchone()
    db.close()
    return dict(row) if row else None


def get_public_key():
    """Retorna apenas a chave publica (para o client-side subscribe)."""
    cfg = get_or_create_vapid_keys()
    return cfg['public_key'] if cfg else None


def save_subscription(org_id, user_id, subscription_json, user_agent=None):
    """
    Armazena uma subscription do service worker.
    subscription_json deve ter {endpoint, keys: {p256dh, auth}}
    """
    endpoint = subscription_json.get('endpoint')
    keys = subscription_json.get('keys') or {}
    p256dh = keys.get('p256dh')
    auth = keys.get('auth')

    if not endpoint or not p256dh or not auth:
        return {'ok': False, 'error': 'subscription invalida'}

    db = get_db()
    db.execute(
        '''INSERT OR REPLACE INTO push_subscriptions
           (org_id, user_id, endpoint, p256dh_key, auth_key, user_agent, is_active)
           VALUES (?, ?, ?, ?, ?, ?, 1)''',
        (org_id, user_id, endpoint, p256dh, auth, user_agent)
    )
    db.commit()
    db.close()
    return {'ok': True}


def send_to_user(org_id, user_id, title, body, url='/dashboard'):
    """Envia push para todas as subscriptions ativas de um usuario."""
    db = get_db()
    subs = db.execute(
        '''SELECT * FROM push_subscriptions
           WHERE org_id=? AND user_id=? AND is_active=1''',
        (org_id, user_id)
    ).fetchall()
    db.close()
    return _dispatch(subs, title, body, url, org_id)


def send_to_org(org_id, title, body, url='/dashboard'):
    """Envia push para todos os usuarios de uma org."""
    db = get_db()
    subs = db.execute(
        'SELECT * FROM push_subscriptions WHERE org_id=? AND is_active=1',
        (org_id,)
    ).fetchall()
    db.close()
    return _dispatch(subs, title, body, url, org_id)


def _dispatch(subscriptions, title, body, url, org_id):
    """Envia para uma lista de subscriptions."""
    if not PYWEBPUSH_AVAILABLE:
        # Modo no-op — grava log mas nao envia
        db = get_db()
        for sub in subscriptions:
            db.execute(
                '''INSERT INTO push_log (org_id, subscription_id, title, body, status, error)
                   VALUES (?, ?, ?, ?, 'noop', 'pywebpush not installed')''',
                (org_id, sub['id'], title, body)
            )
        db.commit()
        db.close()
        return {'ok': False, 'sent': 0, 'error': 'pywebpush not installed — rodar: pip install pywebpush'}

    vapid = get_or_create_vapid_keys()
    if not vapid:
        return {'ok': False, 'error': 'VAPID keys nao disponiveis'}

    sent = 0
    failed = 0
    db = get_db()
    for sub in subscriptions:
        try:
            webpush(
                subscription_info={
                    'endpoint': sub['endpoint'],
                    'keys': {
                        'p256dh': sub['p256dh_key'],
                        'auth': sub['auth_key'],
                    }
                },
                data=json.dumps({'title': title, 'body': body, 'url': url}),
                vapid_private_key=vapid['private_key'],
                vapid_claims={'sub': vapid['subject']}
            )
            sent += 1
            db.execute(
                '''INSERT INTO push_log (org_id, subscription_id, title, body, status)
                   VALUES (?, ?, ?, ?, 'sent')''',
                (org_id, sub['id'], title, body)
            )
            db.execute(
                'UPDATE push_subscriptions SET last_sent_at=datetime("now") WHERE id=?',
                (sub['id'],)
            )
        except WebPushException as e:
            failed += 1
            err = str(e)[:300]
            db.execute(
                '''INSERT INTO push_log (org_id, subscription_id, title, body, status, error)
                   VALUES (?, ?, ?, ?, 'failed', ?)''',
                (org_id, sub['id'], title, body, err)
            )
            # 410 Gone = unsubscribed — marcar como inativa
            if '410' in err or 'gone' in err.lower():
                db.execute(
                    'UPDATE push_subscriptions SET is_active=0 WHERE id=?',
                    (sub['id'],)
                )
        except Exception as e:
            failed += 1
            db.execute(
                '''INSERT INTO push_log (org_id, subscription_id, title, body, status, error)
                   VALUES (?, ?, ?, ?, 'failed', ?)''',
                (org_id, sub['id'], title, body, str(e)[:300])
            )
    db.commit()
    db.close()
    return {'ok': True, 'sent': sent, 'failed': failed}
