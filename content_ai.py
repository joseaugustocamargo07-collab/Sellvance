# content_ai.py — Gerador de conteudo via IA
# Gera titulos, descricoes, bullets e tags para produtos baseado em:
#   - SKU + nome do produto
#   - Categoria
#   - Caracteristicas diferenciais
#   - Marketplace alvo (cada um tem regras diferentes)
#
# Funciona como template engine offline + hook para LLM opcional.

from database import get_db


# Templates por marketplace (max chars e estrutura)
MARKETPLACE_RULES = {
    'mercado_livre': {
        'title_max': 60,
        'description_max': 50000,
        'bullets': False,
        'tags_max': 0,
        'title_template': '{brand} {product} {key_feature} {size}',
    },
    'amazon': {
        'title_max': 200,
        'description_max': 2000,
        'bullets': True,
        'bullets_count': 5,
        'tags_max': 7,
        'title_template': '{brand} {product}, {key_feature}, {size} - {benefit}',
    },
    'shopee': {
        'title_max': 120,
        'description_max': 3000,
        'bullets': False,
        'tags_max': 18,
        'title_template': '{brand} {product} {size} {key_feature} [{tags}]',
    },
    'tiktok_shop': {
        'title_max': 100,
        'description_max': 1500,
        'bullets': False,
        'tags_max': 10,
        'title_template': '{product} {key_feature} ✨ {benefit}',
    },
}


# Palavras-chave que aumentam CTR (baseado em data agregada)
CTR_BOOSTERS = [
    'original', 'novo', 'premium', 'oficial', 'exclusivo',
    'promocao', 'frete gratis', 'garantia', 'brasil', 'nacional',
    'lancamento', 'imperdivel', 'top', 'mais vendido'
]


def ensure_tables():
    """Bootstrap das tabelas de content AI."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS content_generations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            sku             TEXT,
            marketplace     TEXT,
            input_data      TEXT,
            title           TEXT,
            description     TEXT,
            bullets         TEXT,
            tags            TEXT,
            seo_score       INTEGER DEFAULT 0,
            applied          INTEGER DEFAULT 0,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_content_gen_sku
            ON content_generations(org_id, sku, created_at);

        CREATE TABLE IF NOT EXISTS content_templates (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER,
            name            TEXT NOT NULL,
            marketplace     TEXT,
            template_type   TEXT,
            template_text   TEXT,
            is_default      INTEGER DEFAULT 0,
            created_at      TEXT DEFAULT (datetime('now'))
        );
    ''')
    db.commit()
    db.close()


def _clean(text, max_chars):
    """Corta texto no limite sem quebrar palavras."""
    if not text:
        return ''
    text = text.strip()
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars].rsplit(' ', 1)[0]
    return cut


def _seo_score(title, description, tags):
    """
    Calcula score SEO 0-100 baseado em:
      - Presenca de keywords CTR
      - Tamanho otimo do titulo
      - Densidade de keywords na descricao
      - Numero de tags
    """
    score = 0
    title_lower = (title or '').lower()
    desc_lower = (description or '').lower()

    # +10 por keyword booster no titulo
    booster_hits = sum(1 for b in CTR_BOOSTERS if b in title_lower)
    score += min(booster_hits * 10, 30)

    # +20 se tamanho do titulo entre 50-120
    tl = len(title or '')
    if 50 <= tl <= 120:
        score += 20
    elif 30 <= tl <= 150:
        score += 10

    # +20 se descricao > 300 chars
    dl = len(description or '')
    if dl >= 300:
        score += 20
    elif dl >= 100:
        score += 10

    # +15 se tem pelo menos 5 tags
    if tags and len(tags) >= 5:
        score += 15
    elif tags:
        score += 5

    # +15 bonus por "frete gratis", "oficial", "garantia"
    for kw in ('frete gratis', 'oficial', 'garantia'):
        if kw in title_lower or kw in desc_lower:
            score += 5
    score = min(score, 100)
    return score


def generate_title(product_data, marketplace='mercado_livre'):
    """Gera titulo otimizado para o marketplace."""
    rules = MARKETPLACE_RULES.get(marketplace, MARKETPLACE_RULES['mercado_livre'])
    max_chars = rules['title_max']

    brand = product_data.get('brand', '')
    product = product_data.get('name', 'Produto')
    key_feature = product_data.get('key_feature', '')
    size = product_data.get('size', '')
    benefit = product_data.get('benefit', 'Qualidade Premium')
    tags = ' '.join(product_data.get('tags', [])[:3])

    template = rules['title_template']
    title = template.format(
        brand=brand,
        product=product,
        key_feature=key_feature,
        size=size,
        benefit=benefit,
        tags=tags,
    )
    # Limpar espacos duplos
    title = ' '.join(title.split())
    return _clean(title, max_chars)


def generate_description(product_data, marketplace='mercado_livre'):
    """Gera descricao estruturada."""
    rules = MARKETPLACE_RULES.get(marketplace, MARKETPLACE_RULES['mercado_livre'])
    max_chars = rules['description_max']

    name = product_data.get('name', 'Produto')
    features = product_data.get('features', [])
    benefits = product_data.get('benefits', [])
    specs = product_data.get('specs', {})

    parts = []
    parts.append(f'{name} — produto de qualidade premium com garantia.')
    parts.append('')

    if features:
        parts.append('🎯 CARACTERISTICAS:')
        for f in features[:8]:
            parts.append(f'• {f}')
        parts.append('')

    if benefits:
        parts.append('✨ BENEFICIOS:')
        for b in benefits[:5]:
            parts.append(f'• {b}')
        parts.append('')

    if specs:
        parts.append('📋 ESPECIFICACOES:')
        for k, v in list(specs.items())[:10]:
            parts.append(f'• {k}: {v}')
        parts.append('')

    parts.append('🚚 ENVIO RAPIDO E SEGURO')
    parts.append('🔒 COMPRA 100% PROTEGIDA')
    parts.append('⭐ GARANTIA DO VENDEDOR')

    description = '\n'.join(parts)
    return _clean(description, max_chars)


def generate_bullets(product_data, count=5):
    """Gera bullets points (Amazon style)."""
    features = product_data.get('features', [])
    benefits = product_data.get('benefits', [])

    bullets = []
    # Mistura features + benefits priorizando benefits
    combined = benefits[:count//2 + 1] + features[:count]
    for item in combined[:count]:
        bullets.append(f'✓ {item.upper()}' if len(item) < 50 else f'✓ {item}')
    return bullets


def generate_tags(product_data, marketplace='mercado_livre'):
    """Gera tags/keywords baseado no produto."""
    rules = MARKETPLACE_RULES.get(marketplace, MARKETPLACE_RULES['mercado_livre'])
    max_tags = rules['tags_max']
    if max_tags == 0:
        return []

    name = product_data.get('name', '').lower()
    category = product_data.get('category', '').lower()
    brand = product_data.get('brand', '').lower()

    base_tags = set()
    # Extract words from name
    for word in name.split():
        if len(word) > 3:
            base_tags.add(word)
    if category:
        base_tags.add(category)
    if brand:
        base_tags.add(brand)

    # Add booster tags
    boosters = ['original', 'frete gratis', 'garantia']
    for b in boosters:
        if len(base_tags) < max_tags:
            base_tags.add(b)

    return list(base_tags)[:max_tags]


def generate_full(org_id, product_data, marketplace='mercado_livre', save=True):
    """
    Gera pacote completo: titulo, descricao, bullets, tags + score SEO.
    """
    title = generate_title(product_data, marketplace)
    description = generate_description(product_data, marketplace)
    bullets = generate_bullets(product_data) if MARKETPLACE_RULES.get(marketplace, {}).get('bullets') else []
    tags = generate_tags(product_data, marketplace)
    seo_score = _seo_score(title, description, tags)

    result = {
        'title': title,
        'description': description,
        'bullets': bullets,
        'tags': tags,
        'seo_score': seo_score,
        'marketplace': marketplace,
    }

    if save:
        import json as _json
        db = get_db()
        db.execute(
            '''INSERT INTO content_generations
               (org_id, sku, marketplace, input_data, title, description,
                bullets, tags, seo_score)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (org_id, product_data.get('sku'), marketplace,
             _json.dumps(product_data),
             title, description,
             _json.dumps(bullets), _json.dumps(tags), seo_score)
        )
        db.commit()
        db.close()

        try:
            from telemetry import track
            track('action', 'content_generated', marketplace=marketplace, seo_score=seo_score)
        except Exception:
            pass

    return result


def get_recent_generations(org_id, limit=50):
    """Lista geracoes recentes."""
    db = get_db()
    rows = [dict(r) for r in db.execute(
        '''SELECT * FROM content_generations
           WHERE org_id=? ORDER BY id DESC LIMIT ?''',
        (org_id, limit)
    ).fetchall()]
    db.close()
    return rows


def mark_applied(generation_id):
    """Marca que o conteudo foi aplicado ao produto."""
    db = get_db()
    db.execute(
        'UPDATE content_generations SET applied=1 WHERE id=?',
        (generation_id,)
    )
    db.commit()
    db.close()
