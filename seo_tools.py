# seo_tools.py — Ferramentas de SEO para o Sellvance
# Site Audit, On-page Analysis, Keyword Tracking, Content Optimizer
#
# Usa APIs gratuitas:
#   - Google PageSpeed Insights API (sem chave, rate-limited)
#   - Analise on-page via fetch + parse HTML
#   - Keyword position tracking via Google Search Console API (quando conectado)

import json
import urllib.request
import urllib.parse
import re
from database import get_db


# ── Google PageSpeed Insights (gratuito, sem chave) ──────────────────────

PAGESPEED_API = 'https://www.googleapis.com/pagespeedonline/v5/runPagespeed'

import os
import time as _time

def _get_google_api_key():
    """API key do Google Cloud — aumenta limite de 2/min pra 25.000/dia."""
    key = (os.environ.get('GOOGLE_API_KEY', '') or '').strip()
    if not key:
        # Fallback hardcoded temporario enquanto Railway nao carrega env var
        key = 'AIzaSyCopu0kUYOHmdgM_QvXgSNH1aPJQlEnR_k'
    return key


def ensure_tables():
    """Bootstrap das tabelas de SEO."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS seo_audits (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            url             TEXT NOT NULL,
            performance     INTEGER DEFAULT 0,
            accessibility   INTEGER DEFAULT 0,
            best_practices  INTEGER DEFAULT 0,
            seo_score       INTEGER DEFAULT 0,
            fcp_ms          INTEGER DEFAULT 0,
            lcp_ms          INTEGER DEFAULT 0,
            cls             REAL DEFAULT 0,
            tbt_ms          INTEGER DEFAULT 0,
            speed_index     INTEGER DEFAULT 0,
            issues          TEXT DEFAULT '[]',
            opportunities   TEXT DEFAULT '[]',
            raw_data        TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_seo_audits_org
            ON seo_audits(org_id, created_at);

        CREATE TABLE IF NOT EXISTS seo_keywords (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            keyword         TEXT NOT NULL,
            current_position REAL,
            previous_position REAL,
            best_position   REAL,
            url             TEXT,
            impressions     INTEGER DEFAULT 0,
            clicks          INTEGER DEFAULT 0,
            ctr             REAL DEFAULT 0,
            last_checked    TEXT DEFAULT (datetime('now')),
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_seo_keywords_org
            ON seo_keywords(org_id, keyword);

        CREATE TABLE IF NOT EXISTS seo_pages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id          INTEGER NOT NULL,
            url             TEXT NOT NULL,
            title           TEXT,
            meta_desc       TEXT,
            h1              TEXT,
            word_count      INTEGER DEFAULT 0,
            has_schema      INTEGER DEFAULT 0,
            has_og          INTEGER DEFAULT 0,
            has_canonical   INTEGER DEFAULT 0,
            page_score      INTEGER DEFAULT 0,
            issues          TEXT DEFAULT '[]',
            last_analyzed   TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_seo_pages_org
            ON seo_pages(org_id, url);
    ''')
    db.commit()
    db.close()


def run_pagespeed_audit(url, strategy='mobile'):
    """
    Roda Google PageSpeed Insights numa URL.
    strategy: 'mobile' ou 'desktop'
    Retorna scores + metricas Core Web Vitals.
    """
    params = {
        'url': url,
        'strategy': strategy,
        'category': ['performance', 'accessibility', 'best-practices', 'seo'],
        'locale': 'pt_BR',
    }
    api_key = _get_google_api_key()
    if api_key:
        params['key'] = api_key
    query = urllib.parse.urlencode(params, doseq=True)
    api_url = f'{PAGESPEED_API}?{query}'

    # Retry com backoff pra lidar com 429 (rate limit)
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            req = urllib.request.Request(api_url, headers={'User-Agent': 'Sellvance/1.0'})
            with urllib.request.urlopen(req, timeout=90) as resp:
                data = json.loads(resp.read().decode())
            break  # sucesso, sai do loop
        except urllib.error.HTTPError as e:
            err_body = ''
            try:
                err_body = e.read().decode('utf-8', errors='replace')[:1000]
            except Exception:
                pass
            if e.code == 429 and attempt < max_retries:
                _time.sleep(5 * (attempt + 1))
                continue
            if e.code == 429:
                hint = 'Rate limit atingido. Configure GOOGLE_API_KEY no Railway pra 25.000 req/dia.'
                return {'ok': False, 'error': hint}
            if e.code == 400:
                # Google nao conseguiu analisar a pagina (ex: site lento, bloqueado, JS-only)
                try:
                    err_json = json.loads(err_body)
                    msg = err_json.get('error', {}).get('message', 'Erro desconhecido')
                except Exception:
                    msg = err_body[:300]
                return {'ok': False, 'error': f'Google nao conseguiu analisar esta pagina: {msg}', 'code': 400}
            return {'ok': False, 'error': f'HTTP Error {e.code}: {err_body[:200]}'}
        except Exception as e:
            return {'ok': False, 'error': str(e)[:300]}

    # Parse scores
    categories = data.get('lighthouseResult', {}).get('categories', {})
    audits = data.get('lighthouseResult', {}).get('audits', {})

    scores = {
        'performance': int((categories.get('performance', {}).get('score') or 0) * 100),
        'accessibility': int((categories.get('accessibility', {}).get('score') or 0) * 100),
        'best_practices': int((categories.get('best-practices', {}).get('score') or 0) * 100),
        'seo_score': int((categories.get('seo', {}).get('score') or 0) * 100),
    }

    # Core Web Vitals
    metrics = {
        'fcp_ms': int(audits.get('first-contentful-paint', {}).get('numericValue', 0)),
        'lcp_ms': int(audits.get('largest-contentful-paint', {}).get('numericValue', 0)),
        'cls': round(float(audits.get('cumulative-layout-shift', {}).get('numericValue', 0)), 3),
        'tbt_ms': int(audits.get('total-blocking-time', {}).get('numericValue', 0)),
        'speed_index': int(audits.get('speed-index', {}).get('numericValue', 0)),
    }

    # Issues (auditorias que falharam)
    issues = []
    opportunities = []
    for key, audit in audits.items():
        if audit.get('score') is not None and audit['score'] < 0.9:
            item = {
                'id': key,
                'title': audit.get('title', ''),
                'description': audit.get('description', '')[:200],
                'score': audit.get('score', 0),
                'display_value': audit.get('displayValue', ''),
            }
            if audit.get('details', {}).get('type') == 'opportunity':
                opportunities.append(item)
            elif audit['score'] < 0.5:
                issues.append(item)

    issues.sort(key=lambda x: x['score'])
    opportunities.sort(key=lambda x: x['score'])

    return {
        'ok': True,
        'url': url,
        'strategy': strategy,
        **scores,
        **metrics,
        'issues': issues[:15],
        'opportunities': opportunities[:10],
    }


def save_audit(org_id, audit_result):
    """Salva resultado do audit no banco."""
    if not audit_result.get('ok'):
        return
    db = get_db()
    db.execute(
        '''INSERT INTO seo_audits
           (org_id, url, performance, accessibility, best_practices, seo_score,
            fcp_ms, lcp_ms, cls, tbt_ms, speed_index, issues, opportunities)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (org_id, audit_result['url'],
         audit_result['performance'], audit_result['accessibility'],
         audit_result['best_practices'], audit_result['seo_score'],
         audit_result['fcp_ms'], audit_result['lcp_ms'],
         audit_result['cls'], audit_result['tbt_ms'], audit_result['speed_index'],
         json.dumps(audit_result.get('issues', [])),
         json.dumps(audit_result.get('opportunities', [])))
    )
    db.commit()
    db.close()

    try:
        from telemetry import track
        track('action', 'seo_audit_run', url=audit_result['url'],
              performance=audit_result['performance'],
              seo=audit_result['seo_score'])
    except Exception:
        pass


# ── On-page SEO Analyzer ────────────────────────────────────────────────

def analyze_page(url):
    """
    Analisa uma pagina web e retorna score SEO on-page.
    Checa: title, meta desc, H1, word count, schema, og tags, canonical, etc.
    """
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; Sellvance SEO Bot/1.0)'
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        return {'ok': False, 'error': str(e)[:200]}

    issues = []
    score = 100  # comeca em 100, desconta por problema

    # Title
    title_match = re.search(r'<title[^>]*>([^<]+)</title>', html, re.I)
    title = title_match.group(1).strip() if title_match else ''
    if not title:
        issues.append({'type': 'critical', 'msg': 'Pagina sem tag <title>'})
        score -= 20
    elif len(title) < 30:
        issues.append({'type': 'warning', 'msg': f'Title muito curto ({len(title)} chars, ideal: 50-60)'})
        score -= 5
    elif len(title) > 70:
        issues.append({'type': 'warning', 'msg': f'Title muito longo ({len(title)} chars, ideal: 50-60)'})
        score -= 3

    # Meta description
    meta_desc_match = re.search(r'<meta\s+name=["\']description["\']\s+content=["\']([^"\']*)', html, re.I)
    meta_desc = meta_desc_match.group(1).strip() if meta_desc_match else ''
    if not meta_desc:
        issues.append({'type': 'critical', 'msg': 'Sem meta description'})
        score -= 15
    elif len(meta_desc) < 100:
        issues.append({'type': 'warning', 'msg': f'Meta description curta ({len(meta_desc)} chars, ideal: 150-160)'})
        score -= 5
    elif len(meta_desc) > 170:
        issues.append({'type': 'info', 'msg': f'Meta description longa ({len(meta_desc)} chars)'})
        score -= 2

    # H1
    h1_match = re.search(r'<h1[^>]*>([^<]+)</h1>', html, re.I)
    h1 = h1_match.group(1).strip() if h1_match else ''
    h1_count = len(re.findall(r'<h1[^>]*>', html, re.I))
    if not h1:
        issues.append({'type': 'critical', 'msg': 'Sem tag H1'})
        score -= 15
    if h1_count > 1:
        issues.append({'type': 'warning', 'msg': f'Multiplas tags H1 ({h1_count}) — ideal: apenas 1'})
        score -= 5

    # Word count
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text)
    word_count = len(text.split())
    if word_count < 300:
        issues.append({'type': 'warning', 'msg': f'Conteudo curto ({word_count} palavras, ideal: >600)'})
        score -= 10

    # Schema markup (JSON-LD)
    has_schema = bool(re.search(r'application/ld\+json', html, re.I))
    if not has_schema:
        issues.append({'type': 'info', 'msg': 'Sem Schema markup (JSON-LD)'})
        score -= 5

    # Open Graph tags
    has_og = bool(re.search(r'og:title|og:description|og:image', html, re.I))
    if not has_og:
        issues.append({'type': 'warning', 'msg': 'Sem Open Graph tags (og:title, og:image)'})
        score -= 5

    # Canonical
    has_canonical = bool(re.search(r'<link[^>]+rel=["\']canonical["\']', html, re.I))
    if not has_canonical:
        issues.append({'type': 'info', 'msg': 'Sem tag canonical'})
        score -= 3

    # Images sem alt
    imgs = re.findall(r'<img[^>]*>', html, re.I)
    imgs_no_alt = [i for i in imgs if 'alt=' not in i.lower() or 'alt=""' in i.lower()]
    if imgs_no_alt:
        issues.append({'type': 'warning', 'msg': f'{len(imgs_no_alt)} de {len(imgs)} imagens sem alt text'})
        score -= min(len(imgs_no_alt) * 2, 10)

    # HTTPS
    if not url.startswith('https://'):
        issues.append({'type': 'critical', 'msg': 'Site nao usa HTTPS'})
        score -= 15

    # Mobile viewport
    has_viewport = bool(re.search(r'<meta[^>]+viewport', html, re.I))
    if not has_viewport:
        issues.append({'type': 'critical', 'msg': 'Sem meta viewport (nao e mobile-friendly)'})
        score -= 10

    score = max(0, min(100, score))

    return {
        'ok': True,
        'url': url,
        'title': title,
        'meta_desc': meta_desc[:200],
        'h1': h1,
        'word_count': word_count,
        'has_schema': has_schema,
        'has_og': has_og,
        'has_canonical': has_canonical,
        'page_score': score,
        'issues': issues,
        'total_images': len(imgs),
        'images_without_alt': len(imgs_no_alt),
    }


def save_page_analysis(org_id, result):
    """Salva analise on-page no banco."""
    if not result.get('ok'):
        return
    db = get_db()
    existing = db.execute(
        'SELECT id FROM seo_pages WHERE org_id=? AND url=?',
        (org_id, result['url'])
    ).fetchone()
    if existing:
        db.execute(
            '''UPDATE seo_pages SET title=?, meta_desc=?, h1=?, word_count=?,
               has_schema=?, has_og=?, has_canonical=?, page_score=?,
               issues=?, last_analyzed=datetime('now')
               WHERE id=?''',
            (result['title'], result['meta_desc'], result['h1'],
             result['word_count'], int(result['has_schema']),
             int(result['has_og']), int(result['has_canonical']),
             result['page_score'], json.dumps(result['issues']),
             existing['id'])
        )
    else:
        db.execute(
            '''INSERT INTO seo_pages
               (org_id, url, title, meta_desc, h1, word_count, has_schema,
                has_og, has_canonical, page_score, issues)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (org_id, result['url'], result['title'], result['meta_desc'],
             result['h1'], result['word_count'], int(result['has_schema']),
             int(result['has_og']), int(result['has_canonical']),
             result['page_score'], json.dumps(result['issues']))
        )
    db.commit()
    db.close()


# ── Keyword Tracking ────────────────────────────────────────────────────

def add_keyword(org_id, keyword, url=None):
    """Adiciona keyword pra monitorar."""
    db = get_db()
    existing = db.execute(
        'SELECT id FROM seo_keywords WHERE org_id=? AND keyword=?',
        (org_id, keyword.lower().strip())
    ).fetchone()
    if existing:
        db.close()
        return {'ok': False, 'error': 'keyword ja existe'}
    db.execute(
        '''INSERT INTO seo_keywords (org_id, keyword, url)
           VALUES (?, ?, ?)''',
        (org_id, keyword.lower().strip(), url)
    )
    db.commit()
    db.close()
    return {'ok': True}


def get_keywords(org_id):
    db = get_db()
    rows = [dict(r) for r in db.execute(
        'SELECT * FROM seo_keywords WHERE org_id=? ORDER BY current_position ASC NULLS LAST',
        (org_id,)
    ).fetchall()]
    db.close()
    return rows


def get_audit_history(org_id, limit=20):
    db = get_db()
    rows = [dict(r) for r in db.execute(
        'SELECT id, url, performance, seo_score, created_at FROM seo_audits WHERE org_id=? ORDER BY id DESC LIMIT ?',
        (org_id, limit)
    ).fetchall()]
    db.close()
    return rows


def get_pages(org_id):
    db = get_db()
    rows = [dict(r) for r in db.execute(
        'SELECT * FROM seo_pages WHERE org_id=? ORDER BY page_score ASC',
        (org_id,)
    ).fetchall()]
    db.close()
    return rows


# ══════════════════════════════════════════════════════════════════════════
#  ANALISE COMPLETA COM IA — keyword extraction + long-tail + diagnostico
# ══════════════════════════════════════════════════════════════════════════

# Stop words em portugues (filtrar palavras sem valor SEO)
_STOP_WORDS_PT = set('''
    a ao aos as com da das de do dos e em es eu ha ja la lhe lhes lo ma mas me
    meu na nas nao nem no nos o os ou para pela pelas pelo pelos por qual quando
    que se sem seu sua te tem um uma uns umas voce nos na no pra pro isso essa
    esse esta este foi ser ter como mais entre sobre ate pode podem tambem ja
    ainda so sao era eram foi foram muito bem aqui ali onde tudo todo toda todos
    site www http https html php com br org net page home contato sobre
'''.split())


def _extract_keywords_from_html(html, url=''):
    """
    Extrai keywords relevantes do conteudo HTML.
    Analisa: title, H1-H3, meta keywords, texto visivel, alt text.
    Retorna lista ordenada por frequencia/relevancia.
    """
    keywords = {}

    # Peso por onde a palavra aparece
    WEIGHTS = {'title': 5, 'h1': 4, 'h2': 3, 'h3': 2, 'meta_kw': 3, 'body': 1, 'alt': 2}

    # Title
    title_match = re.search(r'<title[^>]*>([^<]+)</title>', html, re.I)
    if title_match:
        for w in _tokenize(title_match.group(1)):
            keywords[w] = keywords.get(w, 0) + WEIGHTS['title']

    # H1-H3
    for tag, weight_key in [('h1', 'h1'), ('h2', 'h2'), ('h3', 'h3')]:
        for m in re.finditer(rf'<{tag}[^>]*>(.*?)</{tag}>', html, re.I | re.S):
            text = re.sub(r'<[^>]+>', '', m.group(1))
            for w in _tokenize(text):
                keywords[w] = keywords.get(w, 0) + WEIGHTS[weight_key]

    # Meta keywords
    kw_match = re.search(r'<meta\s+name=["\']keywords["\']\s+content=["\']([^"\']*)', html, re.I)
    if kw_match:
        for w in _tokenize(kw_match.group(1)):
            keywords[w] = keywords.get(w, 0) + WEIGHTS['meta_kw']

    # Body text (peso menor)
    body_match = re.search(r'<body[^>]*>(.*?)</body>', html, re.I | re.S)
    if body_match:
        text = re.sub(r'<script[^>]*>.*?</script>', '', body_match.group(1), flags=re.I | re.S)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.I | re.S)
        text = re.sub(r'<[^>]+>', ' ', text)
        for w in _tokenize(text):
            keywords[w] = keywords.get(w, 0) + WEIGHTS['body']

    # Alt text de imagens
    for m in re.finditer(r'alt=["\']([^"\']+)["\']', html, re.I):
        for w in _tokenize(m.group(1)):
            keywords[w] = keywords.get(w, 0) + WEIGHTS['alt']

    # Filtrar e ordenar
    filtered = {k: v for k, v in keywords.items()
                if len(k) > 3 and k not in _STOP_WORDS_PT and v >= 2}
    sorted_kw = sorted(filtered.items(), key=lambda x: x[1], reverse=True)
    return [{'keyword': k, 'relevance': v} for k, v in sorted_kw[:30]]


def _tokenize(text):
    """Tokeniza texto em palavras normalizadas."""
    text = text.lower()
    text = re.sub(r'[^a-záàâãéèêíïóôõúüç\s]', ' ', text)
    words = text.split()
    return [w for w in words if len(w) > 2 and w not in _STOP_WORDS_PT]


def _extract_phrases(html, min_words=3, max_words=6):
    """
    Extrai frases de cauda longa (long-tail) do conteudo.
    Prioriza frases de H1, H2, title e paragrafos.
    """
    phrases = {}

    # Fontes de frases
    sources = []
    # Title
    title_m = re.search(r'<title[^>]*>([^<]+)</title>', html, re.I)
    if title_m:
        sources.append((title_m.group(1), 5))
    # H1-H3
    for tag, weight in [('h1', 4), ('h2', 3), ('h3', 2)]:
        for m in re.finditer(rf'<{tag}[^>]*>(.*?)</{tag}>', html, re.I | re.S):
            text = re.sub(r'<[^>]+>', '', m.group(1))
            sources.append((text, weight))
    # Paragrafos
    for m in re.finditer(r'<p[^>]*>(.*?)</p>', html, re.I | re.S):
        text = re.sub(r'<[^>]+>', '', m.group(1))
        if len(text) > 30:
            sources.append((text, 1))

    for text, weight in sources:
        text = text.lower().strip()
        text = re.sub(r'[^a-záàâãéèêíïóôõúüç\s]', ' ', text)
        words = [w for w in text.split() if len(w) > 2 and w not in _STOP_WORDS_PT]
        # Gerar n-grams
        for n in range(min_words, min(max_words + 1, len(words) + 1)):
            for i in range(len(words) - n + 1):
                phrase = ' '.join(words[i:i+n])
                if len(phrase) > 10:
                    phrases[phrase] = phrases.get(phrase, 0) + weight

    sorted_phrases = sorted(phrases.items(), key=lambda x: x[1], reverse=True)
    return [{'phrase': p, 'relevance': v} for p, v in sorted_phrases[:20]]


def _generate_keyword_suggestions(extracted_keywords, title='', niche=''):
    """
    Gera sugestoes de keywords baseado nas extraidas + nicho.
    Adiciona variações de long-tail e perguntas comuns.
    """
    suggestions = []
    top_keywords = [k['keyword'] for k in extracted_keywords[:8]]

    # Prefixos de busca comuns no Brasil
    prefixes = ['melhor', 'como', 'onde', 'qual', 'quanto custa', 'preço']
    suffixes = ['perto de mim', 'online', 'em {city}', 'barato', 'profissional',
                'de qualidade', 'confiável', 'avaliação', 'opinião']
    questions = ['como funciona {kw}', 'quanto custa {kw}', 'qual melhor {kw}',
                 '{kw} vale a pena', '{kw} funciona mesmo', 'onde encontrar {kw}',
                 '{kw} antes e depois', '{kw} resultados']

    for kw in top_keywords[:5]:
        # Variações com prefixo
        for p in prefixes[:3]:
            suggestions.append({
                'keyword': f'{p} {kw}',
                'type': 'long_tail',
                'intent': 'informacional' if p in ('como', 'onde', 'qual') else 'transacional',
            })
        # Variações com sufixo
        for s in suffixes[:3]:
            suggestions.append({
                'keyword': f'{kw} {s}'.replace('{city}', 'São Paulo'),
                'type': 'local' if 'perto' in s or '{city}' in s else 'long_tail',
                'intent': 'transacional',
            })
        # Perguntas
        for q in questions[:3]:
            suggestions.append({
                'keyword': q.format(kw=kw),
                'type': 'pergunta',
                'intent': 'informacional',
            })

    # Remover duplicatas preservando ordem
    seen = set()
    unique = []
    for s in suggestions:
        if s['keyword'] not in seen:
            seen.add(s['keyword'])
            unique.append(s)

    return unique[:25]


def _generate_ai_diagnosis(page_data, pagespeed_data=None):
    """
    Gera diagnostico inteligente com recomendacoes priorizadas.
    Analisa todos os dados coletados e produz um relatorio acionavel em PT-BR.
    """
    diagnosis = {
        'score_geral': 0,
        'resumo': '',
        'problemas_criticos': [],
        'melhorias_recomendadas': [],
        'oportunidades_keywords': [],
        'proximos_passos': [],
    }

    issues = page_data.get('issues', [])
    title = page_data.get('title', '')
    meta_desc = page_data.get('meta_desc', '')
    h1 = page_data.get('h1', '')
    word_count = page_data.get('word_count', 0)
    page_score = page_data.get('page_score', 0)
    keywords = page_data.get('extracted_keywords', [])
    phrases = page_data.get('long_tail_phrases', [])

    # Score geral ponderado
    perf_score = pagespeed_data.get('performance', 0) if pagespeed_data else 50
    seo_score = pagespeed_data.get('seo_score', 0) if pagespeed_data else 50
    diagnosis['score_geral'] = int(page_score * 0.4 + perf_score * 0.3 + seo_score * 0.3)

    # ── Problemas criticos ──
    for issue in issues:
        if issue.get('type') == 'critical':
            diagnosis['problemas_criticos'].append({
                'problema': issue['msg'],
                'impacto': 'Alto — afeta diretamente o posicionamento no Google',
                'como_resolver': _get_fix_suggestion(issue['msg']),
            })

    # ── Analise do Title ──
    if title:
        top_kw = keywords[0]['keyword'] if keywords else ''
        if top_kw and top_kw not in title.lower():
            diagnosis['melhorias_recomendadas'].append({
                'area': 'Title',
                'problema': f'A keyword principal "{top_kw}" nao aparece no titulo da pagina',
                'sugestao': f'Inclua "{top_kw}" no inicio do title. Ex: "{top_kw.title()} — {title[:30]}..."',
                'prioridade': 'Alta',
            })
        if len(title) < 40:
            diagnosis['melhorias_recomendadas'].append({
                'area': 'Title',
                'problema': f'Title muito curto ({len(title)} caracteres)',
                'sugestao': 'Expanda o title pra 50-60 caracteres incluindo a keyword principal e um beneficio',
                'prioridade': 'Media',
            })

    # ── Analise da Meta Description ──
    if meta_desc:
        if len(meta_desc) < 120:
            diagnosis['melhorias_recomendadas'].append({
                'area': 'Meta Description',
                'problema': f'Meta description curta ({len(meta_desc)} chars)',
                'sugestao': 'Expanda pra 150-160 chars. Inclua keyword principal + CTA (call-to-action). Ex: "Saiba mais", "Agende agora", "Confira"',
                'prioridade': 'Media',
            })
    else:
        diagnosis['melhorias_recomendadas'].append({
            'area': 'Meta Description',
            'problema': 'Pagina sem meta description',
            'sugestao': 'Adicione <meta name="description" content="..."> com 150-160 chars descrevendo o conteudo + CTA',
            'prioridade': 'Alta',
        })

    # ── Analise do H1 ──
    if not h1:
        diagnosis['melhorias_recomendadas'].append({
            'area': 'H1',
            'problema': 'Pagina sem tag H1',
            'sugestao': 'Adicione um unico H1 na pagina com a keyword principal. O H1 deve ser diferente do title.',
            'prioridade': 'Alta',
        })
    elif h1 == title:
        diagnosis['melhorias_recomendadas'].append({
            'area': 'H1',
            'problema': 'H1 identico ao title — perda de oportunidade de rankear pra variacao da keyword',
            'sugestao': f'Mude o H1 pra uma variacao. Title: "{title[:40]}..." → H1 pode usar sinonimos ou long-tail',
            'prioridade': 'Baixa',
        })

    # ── Analise de conteudo ──
    if word_count < 300:
        diagnosis['melhorias_recomendadas'].append({
            'area': 'Conteudo',
            'problema': f'Conteudo muito curto ({word_count} palavras)',
            'sugestao': 'Paginas com >600 palavras rankeiam em media 2x melhor. Adicione secoes com FAQ, depoimentos, detalhes do servico/produto.',
            'prioridade': 'Alta',
        })
    elif word_count < 600:
        diagnosis['melhorias_recomendadas'].append({
            'area': 'Conteudo',
            'problema': f'Conteudo abaixo do ideal ({word_count} palavras)',
            'sugestao': 'Ideal >600 palavras. Adicione FAQ com perguntas frequentes (excelente pra long-tail), descricoes detalhadas, e CTAs.',
            'prioridade': 'Media',
        })

    # ── Performance ──
    if pagespeed_data:
        lcp = pagespeed_data.get('lcp_ms', 0)
        if lcp > 4000:
            diagnosis['problemas_criticos'].append({
                'problema': f'LCP (Largest Contentful Paint) de {lcp}ms — muito lento (ideal: <2500ms)',
                'impacto': 'Critico — Google penaliza sites lentos no ranking desde 2021 (Core Web Vitals)',
                'como_resolver': 'Comprima imagens (WebP), use lazy loading, ative cache do servidor, considere CDN (Cloudflare gratis)',
            })
        fcp = pagespeed_data.get('fcp_ms', 0)
        if fcp > 3000:
            diagnosis['melhorias_recomendadas'].append({
                'area': 'Performance',
                'problema': f'FCP (First Contentful Paint) de {fcp}ms — lento',
                'sugestao': 'Reduza CSS/JS bloqueante, use font-display:swap, minimize recursos carregados antes do primeiro render',
                'prioridade': 'Alta',
            })

    # ── Oportunidades de keywords ──
    if keywords:
        top_5 = keywords[:5]
        for kw in top_5:
            diagnosis['oportunidades_keywords'].append({
                'keyword': kw['keyword'],
                'relevancia': kw['relevance'],
                'dica': f'Aparece {kw["relevance"]}x no site. {"Ja esta bem posicionada." if kw["relevance"] >= 5 else "Aumente a frequencia naturalmente no conteudo."}',
            })

    if phrases:
        diagnosis['oportunidades_keywords'].append({
            'keyword': 'FRASES LONG-TAIL DETECTADAS',
            'relevancia': 0,
            'dica': f'{len(phrases)} frases de cauda longa encontradas. Long-tail tem menos concorrencia e maior taxa de conversao.',
        })

    # ── Proximos passos priorizados ──
    if diagnosis['problemas_criticos']:
        diagnosis['proximos_passos'].append(
            '🔴 URGENTE: Resolva os problemas criticos listados acima — eles estao impedindo seu ranking'
        )
    if not page_data.get('has_schema'):
        diagnosis['proximos_passos'].append(
            '📋 Adicione Schema markup (JSON-LD) — ajuda o Google a entender seu conteudo e pode gerar rich snippets'
        )
    if not page_data.get('has_og'):
        diagnosis['proximos_passos'].append(
            '📱 Adicione Open Graph tags — melhora como seu link aparece quando compartilhado no Facebook/WhatsApp/LinkedIn'
        )
    if word_count < 600:
        diagnosis['proximos_passos'].append(
            '✍️ Expanda o conteudo pra >600 palavras com secoes de FAQ, depoimentos e CTAs'
        )
    if keywords:
        diagnosis['proximos_passos'].append(
            f'🎯 Foque na keyword principal: "{keywords[0]["keyword"]}" — use ela no title, H1, primeiro paragrafo e alt text das imagens'
        )
    diagnosis['proximos_passos'].append(
        '🔄 Rode este audit novamente em 30 dias pra medir progresso'
    )

    # ── Resumo ──
    score = diagnosis['score_geral']
    n_crit = len(diagnosis['problemas_criticos'])
    n_melh = len(diagnosis['melhorias_recomendadas'])
    if score >= 80:
        diagnosis['resumo'] = f'Site em boa forma ({score}/100). {n_melh} melhorias sugeridas pra chegar ao topo.'
    elif score >= 50:
        diagnosis['resumo'] = f'Site precisa de atencao ({score}/100). {n_crit} problemas criticos e {n_melh} melhorias recomendadas.'
    else:
        diagnosis['resumo'] = f'Site com problemas serios ({score}/100). {n_crit} problemas criticos precisam ser resolvidos antes de investir em trafego.'

    return diagnosis


def _get_fix_suggestion(issue_msg):
    """Retorna sugestao de correcao pra um problema conhecido."""
    msg = issue_msg.lower()
    if 'title' in msg:
        return 'Adicione uma tag <title> unica e descritiva com 50-60 caracteres incluindo sua keyword principal.'
    if 'meta description' in msg:
        return 'Adicione <meta name="description" content="..."> com 150-160 caracteres. Inclua keyword + beneficio + CTA.'
    if 'h1' in msg:
        return 'Adicione uma unica tag <h1> com sua keyword principal. Deve ser diferente do title.'
    if 'https' in msg:
        return 'Instale certificado SSL (gratis via Let\'s Encrypt). Google penaliza sites HTTP desde 2018.'
    if 'viewport' in msg:
        return 'Adicione <meta name="viewport" content="width=device-width, initial-scale=1.0"> no <head>.'
    if 'alt' in msg:
        return 'Adicione atributo alt="" descritivo em todas as imagens. Use a keyword principal quando relevante.'
    if 'schema' in msg:
        return 'Adicione JSON-LD com @type apropriado (LocalBusiness, Product, Article, etc). Use schema.org pra referencia.'
    return 'Corrija conforme as boas praticas de SEO atuais.'


def run_full_analysis(url, strategy='mobile'):
    """
    ANALISE COMPLETA — combina tudo:
    1. PageSpeed Insights (performance + core web vitals)
    2. On-page SEO (title, H1, meta, etc)
    3. Keyword extraction (palavras relevantes do conteudo)
    4. Long-tail phrases (frases de cauda longa)
    5. Keyword suggestions (variações e perguntas)
    6. Diagnostico IA (analise inteligente com recomendacoes)
    """
    result = {'ok': True, 'url': url}

    # 1. PageSpeed
    pagespeed = run_pagespeed_audit(url, strategy)
    result['pagespeed'] = pagespeed if pagespeed.get('ok') else {'error': pagespeed.get('error', 'falhou')}

    # 2. On-page
    onpage = analyze_page(url)
    result['onpage'] = onpage if onpage.get('ok') else {'error': onpage.get('error', 'falhou')}

    # 3. Keywords (precisa do HTML)
    if onpage.get('ok'):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; Sellvance SEO Bot/1.0)'
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode('utf-8', errors='replace')
        except Exception:
            html = ''

        if html:
            extracted = _extract_keywords_from_html(html, url)
            phrases = _extract_phrases(html)
            suggestions = _generate_keyword_suggestions(extracted, title=onpage.get('title', ''))
            result['keywords'] = {
                'extracted': extracted,
                'long_tail_phrases': phrases,
                'suggestions': suggestions,
            }
            # Enriquecer onpage com keywords pra IA
            onpage['extracted_keywords'] = extracted
            onpage['long_tail_phrases'] = phrases
        else:
            result['keywords'] = {'extracted': [], 'long_tail_phrases': [], 'suggestions': []}
    else:
        result['keywords'] = {'extracted': [], 'long_tail_phrases': [], 'suggestions': []}

    # 4. Diagnostico IA
    if onpage.get('ok'):
        ps_data = pagespeed if pagespeed.get('ok') else None
        diagnosis = _generate_ai_diagnosis(onpage, ps_data)
        result['diagnosis'] = diagnosis
    else:
        result['diagnosis'] = {'score_geral': 0, 'resumo': 'Nao foi possivel analisar a pagina.'}

    return result
