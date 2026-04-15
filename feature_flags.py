# feature_flags.py — Sistema de toggles para deploys graduais
# Permite ativar/desativar features sem restart, fazer A/B tests
# e rollback instantaneo caso uma feature cause problema.

import hashlib
from database import get_db


# Flags conhecidas (documentacao + fallback default)
# Quando adicionar feature nova, declare aqui primeiro.
KNOWN_FLAGS = {
    'ai_pricing':           {'default': False, 'desc': 'IA de precificacao dinamica'},
    'whatsapp_agent':       {'default': False, 'desc': 'Agente autonomo de WhatsApp'},
    'buybox_monitor':       {'default': False, 'desc': 'Monitor de Buy Box em tempo real'},
    'fraud_detector':       {'default': False, 'desc': 'Deteccao de fraude em devolucoes'},
    'tiktok_live_sync':     {'default': False, 'desc': 'Sincronizacao com TikTok Shop Live'},
    'ai_content_gen':       {'default': False, 'desc': 'Geracao de conteudo via IA'},
    'auto_insights':        {'default': True,  'desc': 'Analises automaticas da plataforma'},
    'advanced_analytics':   {'default': True,  'desc': 'Analytics avancado com coortes'},
}


def ensure_tables():
    """Bootstrap das tabelas de feature flags."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS feature_flags (
            flag_name    TEXT PRIMARY KEY,
            enabled      INTEGER DEFAULT 0,
            rollout_pct  INTEGER DEFAULT 0,
            org_whitelist TEXT DEFAULT '',
            description  TEXT,
            updated_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS feature_flag_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            flag_name  TEXT NOT NULL,
            action     TEXT NOT NULL,
            old_value  TEXT,
            new_value  TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
    ''')

    # Seed flags conhecidas (apenas se nao existirem)
    for name, meta in KNOWN_FLAGS.items():
        existing = db.execute(
            'SELECT flag_name FROM feature_flags WHERE flag_name = ?',
            (name,)
        ).fetchone()
        if not existing:
            db.execute(
                '''INSERT INTO feature_flags (flag_name, enabled, rollout_pct, description)
                   VALUES (?, ?, ?, ?)''',
                (name, 1 if meta['default'] else 0,
                 100 if meta['default'] else 0,
                 meta['desc'])
            )
    db.commit()
    db.close()


def is_enabled(flag_name, org_id=None):
    """
    Checa se uma feature esta habilitada para a org.
    Logica:
      1. Se org esta no whitelist -> True
      2. Se enabled=0 -> False
      3. Se rollout_pct=100 -> True
      4. Senao, hash determinstico do org_id decide (A/B test estavel)
    """
    try:
        db = get_db()
        row = db.execute(
            'SELECT enabled, rollout_pct, org_whitelist FROM feature_flags WHERE flag_name = ?',
            (flag_name,)
        ).fetchone()
        db.close()

        if not row:
            # Flag nao existe no banco — usa default conhecido
            return KNOWN_FLAGS.get(flag_name, {}).get('default', False)

        enabled, pct, whitelist = row['enabled'], row['rollout_pct'] or 0, row['org_whitelist'] or ''

        # Whitelist tem prioridade
        if org_id and whitelist:
            wl_ids = [x.strip() for x in whitelist.split(',') if x.strip()]
            if str(org_id) in wl_ids:
                return True

        if not enabled:
            return False

        if pct >= 100:
            return True
        if pct <= 0:
            return False

        # Hash determinstico: mesmo org sempre cai no mesmo bucket
        if org_id is None:
            return False
        h = int(hashlib.md5(f'{flag_name}:{org_id}'.encode()).hexdigest(), 16)
        return (h % 100) < pct
    except Exception:
        return KNOWN_FLAGS.get(flag_name, {}).get('default', False)


def set_flag(flag_name, enabled=None, rollout_pct=None, whitelist=None):
    """Atualiza uma flag (admin)."""
    db = get_db()
    old = db.execute(
        'SELECT enabled, rollout_pct, org_whitelist FROM feature_flags WHERE flag_name = ?',
        (flag_name,)
    ).fetchone()
    if not old:
        db.execute(
            'INSERT INTO feature_flags (flag_name, enabled, rollout_pct, org_whitelist) VALUES (?, 0, 0, "")',
            (flag_name,)
        )
        old = {'enabled': 0, 'rollout_pct': 0, 'org_whitelist': ''}
    else:
        old = dict(old)

    new_enabled = int(enabled) if enabled is not None else old['enabled']
    new_pct     = int(rollout_pct) if rollout_pct is not None else old['rollout_pct']
    new_wl      = whitelist if whitelist is not None else old['org_whitelist']

    db.execute(
        '''UPDATE feature_flags
           SET enabled=?, rollout_pct=?, org_whitelist=?, updated_at=datetime('now')
           WHERE flag_name=?''',
        (new_enabled, new_pct, new_wl, flag_name)
    )
    db.execute(
        '''INSERT INTO feature_flag_events (flag_name, action, old_value, new_value)
           VALUES (?, 'update', ?, ?)''',
        (flag_name,
         f"enabled={old['enabled']},pct={old['rollout_pct']}",
         f"enabled={new_enabled},pct={new_pct}")
    )
    db.commit()
    db.close()
    return True


def all_flags():
    """Retorna todas as flags (para admin UI)."""
    db = get_db()
    rows = [dict(r) for r in db.execute(
        'SELECT * FROM feature_flags ORDER BY flag_name'
    ).fetchall()]
    db.close()
    # Enriquece com descricao do KNOWN_FLAGS
    for r in rows:
        if r['flag_name'] in KNOWN_FLAGS:
            r['default'] = KNOWN_FLAGS[r['flag_name']]['default']
    return rows


def rollback(flag_name):
    """Desativa instantaneamente uma flag (rollback de emergencia)."""
    set_flag(flag_name, enabled=0, rollout_pct=0)
    return True


def gradual_rollout(flag_name, target_pct):
    """
    Aumenta gradualmente o rollout de uma flag.
    Usado pelo auto_insights quando uma feature tem KPI positivo.
    """
    db = get_db()
    row = db.execute(
        'SELECT rollout_pct FROM feature_flags WHERE flag_name = ?',
        (flag_name,)
    ).fetchone()
    db.close()
    if not row:
        return False
    current = row['rollout_pct'] or 0
    if target_pct > current:
        set_flag(flag_name, enabled=1, rollout_pct=target_pct)
        return True
    return False
