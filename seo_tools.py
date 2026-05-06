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
