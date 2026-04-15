# whatsapp_agent.py — Agente WhatsApp autonomo para atendimento
# Responde perguntas, recupera carrinho abandonado, confirma pedidos.
#
# Arquitetura:
#   - Cada mensagem cai em conversation_log
#   - Classifier decide intencao (pergunta, compra, reclamacao, etc.)
#   - Response engine gera resposta (template ou LLM)
#   - Handoff para humano quando confidence < 0.6

import json
from database import get_db


# Intencoes suportadas + templates de resposta
INTENT_PATTERNS = {
    'greeting': {
        'keywords': ['oi', 'ola', 'bom dia', 'boa tarde', 'boa noite', 'ei'],
        'response': 'Ola {name}! Sou o assistente virtual da {store}. Como posso ajudar hoje?',
        'confidence': 0.95,
    },
    'order_status': {
        'keywords': ['pedido', 'compra', 'encomenda', 'rastreamento', 'entrega', 'chegou'],
        'response': 'Posso verificar seu pedido! Pode me passar o numero do pedido ou o email usado na compra?',
        'confidence': 0.85,
    },
    'price_question': {
        'keywords': ['preco', 'valor', 'quanto', 'custa', 'custo'],
        'response': 'Claro! Qual produto voce quer saber o preco? Me fala o nome ou SKU.',
        'confidence': 0.80,
    },
    'stock_question': {
        'keywords': ['tem', 'disponivel', 'estoque', 'sobrou'],
        'response': 'Vou verificar a disponibilidade. Qual produto voce procura?',
        'confidence': 0.80,
    },
    'complaint': {
        'keywords': ['reclama', 'problema', 'defeito', 'quebra', 'nao funciona', 'insatisfeito'],
        'response': 'Sinto muito pelo ocorrido, {name}. Vou direcionar voce para um atendente especializado agora mesmo. Um momento, por favor.',
        'confidence': 0.90,
        'handoff': True,
    },
    'discount_request': {
        'keywords': ['desconto', 'cupom', 'promocao', 'oferta', 'mais barato'],
        'response': 'Temos sim! Posso oferecer 10% OFF se voce finalizar o pedido agora com o cupom: BEMVINDO10. Quer conferir os produtos?',
        'confidence': 0.85,
    },
    'payment_issue': {
        'keywords': ['pagamento', 'cartao', 'pix', 'boleto', 'pagar'],
        'response': 'Vou te ajudar com o pagamento. Qual forma voce prefere? Temos Pix (5% OFF), cartao e boleto.',
        'confidence': 0.80,
    },
    'cart_recovery': {
        'keywords': [],  # disparado programaticamente
        'response': 'Ei {name}! Vi que voce deixou {product} no carrinho. Quer que eu finalize com 10% OFF? Cupom: VOLTA10',
        'confidence': 1.0,
    },
}


def ensure_tables():
    """Bootstrap das tabelas do agente WhatsApp."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS wa_conversations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            contact_phone   TEXT NOT NULL,
            contact_name    TEXT,
            status          TEXT DEFAULT 'active',
            last_message_at TEXT DEFAULT (datetime('now')),
            handoff_to_human INTEGER DEFAULT 0,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_wa_conv_phone
            ON wa_conversations(org_id, contact_phone);

        CREATE TABLE IF NOT EXISTS wa_messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id INTEGER REFERENCES wa_conversations(id),
            direction       TEXT NOT NULL,
            content         TEXT NOT NULL,
            intent          TEXT,
            confidence      REAL DEFAULT 0,
            handled_by      TEXT DEFAULT 'ai',
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS wa_agent_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            date            TEXT NOT NULL,
            messages_in     INTEGER DEFAULT 0,
            messages_out    INTEGER DEFAULT 0,
            ai_handled      INTEGER DEFAULT 0,
            human_handoff   INTEGER DEFAULT 0,
            conversions     INTEGER DEFAULT 0,
            revenue         REAL DEFAULT 0
        );
    ''')
    db.commit()
    db.close()


def classify_intent(text):
    """
    Classificador simples baseado em keywords.
    Em producao pode ser substituido por chamada a LLM.
    """
    if not text:
        return None, 0.0
    text_lower = text.lower().strip()

    scores = {}
    for intent, data in INTENT_PATTERNS.items():
        if not data['keywords']:
            continue
        matches = sum(1 for kw in data['keywords'] if kw in text_lower)
        if matches > 0:
            scores[intent] = matches * data['confidence']

    if not scores:
        return 'unknown', 0.3

    best_intent = max(scores, key=scores.get)
    confidence = min(scores[best_intent], 1.0)
    return best_intent, confidence


def generate_response(intent, context=None):
    """Gera resposta para uma intencao detectada."""
    context = context or {}
    if intent not in INTENT_PATTERNS:
        return {
            'text': 'Nao consegui entender. Pode reformular ou digitar "atendente" para falar com um humano?',
            'handoff': False,
            'confidence': 0.3
        }
    data = INTENT_PATTERNS[intent]
    text = data['response'].format(
        name=context.get('name', 'cliente'),
        store=context.get('store', 'nossa loja'),
        product=context.get('product', 'seus itens'),
    )
    return {
        'text': text,
        'handoff': data.get('handoff', False),
        'confidence': data.get('confidence', 0.5),
    }


def handle_incoming_message(org_id, contact_phone, contact_name, text):
    """
    Processa mensagem recebida do WhatsApp.
    Retorna a resposta gerada (ou None se handoff para humano).
    """
    from feature_flags import is_enabled
    if not is_enabled('whatsapp_agent', org_id):
        return None  # agente desativado para essa org

    db = get_db()

    # Get or create conversation
    conv = db.execute(
        '''SELECT * FROM wa_conversations
           WHERE org_id=? AND contact_phone=?
           ORDER BY id DESC LIMIT 1''',
        (org_id, contact_phone)
    ).fetchone()
    if not conv:
        cur = db.execute(
            '''INSERT INTO wa_conversations (org_id, contact_phone, contact_name)
               VALUES (?, ?, ?)''',
            (org_id, contact_phone, contact_name)
        )
        conv_id = cur.lastrowid
        handoff = False
    else:
        conv_id = conv['id']
        handoff = bool(conv['handoff_to_human'])
        db.execute(
            'UPDATE wa_conversations SET last_message_at=datetime("now") WHERE id=?',
            (conv_id,)
        )

    # Log incoming
    intent, confidence = classify_intent(text)
    db.execute(
        '''INSERT INTO wa_messages (conversation_id, direction, content, intent, confidence, handled_by)
           VALUES (?, 'in', ?, ?, ?, 'user')''',
        (conv_id, text, intent, confidence)
    )

    if handoff:
        db.commit()
        db.close()
        return None  # humano ja esta cuidando

    # Get org name
    org = db.execute('SELECT name FROM organizations WHERE id=?', (org_id,)).fetchone()
    store_name = org['name'] if org else 'nossa loja'

    response = generate_response(intent, {
        'name': (contact_name or 'cliente').split()[0],
        'store': store_name,
    })

    # Log outgoing
    db.execute(
        '''INSERT INTO wa_messages (conversation_id, direction, content, intent, confidence, handled_by)
           VALUES (?, 'out', ?, ?, ?, 'ai')''',
        (conv_id, response['text'], intent, response['confidence'])
    )

    # Handoff se confidence baixa ou intent pede humano
    if response['handoff'] or response['confidence'] < 0.6:
        db.execute(
            'UPDATE wa_conversations SET handoff_to_human=1 WHERE id=?',
            (conv_id,)
        )

    db.commit()
    db.close()

    try:
        from telemetry import track
        track('action', 'wa_agent_response', intent=intent, confidence=response['confidence'])
    except Exception:
        pass

    return response


def get_agent_stats(org_id, days=7):
    """Estatisticas do agente para dashboard."""
    db = get_db()
    total_in = db.execute(
        '''SELECT COUNT(*) c FROM wa_messages m
           JOIN wa_conversations c ON m.conversation_id = c.id
           WHERE c.org_id=? AND m.direction='in'
           AND m.created_at > datetime('now', ?)''',
        (org_id, f'-{days} days')
    ).fetchone()
    total_out = db.execute(
        '''SELECT COUNT(*) c FROM wa_messages m
           JOIN wa_conversations c ON m.conversation_id = c.id
           WHERE c.org_id=? AND m.direction='out' AND m.handled_by='ai'
           AND m.created_at > datetime('now', ?)''',
        (org_id, f'-{days} days')
    ).fetchone()
    handoffs = db.execute(
        '''SELECT COUNT(*) c FROM wa_conversations
           WHERE org_id=? AND handoff_to_human=1
           AND created_at > datetime('now', ?)''',
        (org_id, f'-{days} days')
    ).fetchone()
    db.close()
    return {
        'messages_in': total_in['c'] if total_in else 0,
        'ai_responses': total_out['c'] if total_out else 0,
        'human_handoffs': handoffs['c'] if handoffs else 0,
        'deflection_rate': round(
            (total_out['c'] / max(total_in['c'], 1)) * 100 if total_in and total_in['c'] else 0, 1
        ),
        'period_days': days,
    }


def get_conversations(org_id, limit=50):
    """Lista conversas recentes."""
    db = get_db()
    rows = [dict(r) for r in db.execute(
        '''SELECT c.*, COUNT(m.id) msg_count
           FROM wa_conversations c
           LEFT JOIN wa_messages m ON m.conversation_id = c.id
           WHERE c.org_id=?
           GROUP BY c.id
           ORDER BY c.last_message_at DESC
           LIMIT ?''',
        (org_id, limit)
    ).fetchall()]
    db.close()
    return rows
