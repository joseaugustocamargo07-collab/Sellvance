"""
Sellvance — OAuth Manager
Gerencia autenticação OAuth 2.0 e API Keys para todas as plataformas.
O dono do Sellvance configura as credenciais do app UMA VEZ via variáveis de ambiente.
Cada cliente faz login na plataforma e o token fica salvo no banco vinculado à org.
"""

import os
import json
import urllib.request
import urllib.parse
import urllib.error
from database import get_db

# ── CONFIGURAÇÕES DOS APPS (você configura 1x no servidor) ────────────────────
# Em produção, use variáveis de ambiente. Em dev, valores de fallback para teste.

OAUTH_APPS = {
    'mercado_livre': {
        'name':          'Mercado Livre',
        'client_id':     os.environ.get('ML_APP_ID',       'SEU_APP_ID_AQUI'),
        'client_secret': os.environ.get('ML_APP_SECRET',   'SEU_SECRET_AQUI'),
        'auth_url':      'https://auth.mercadolibre.com.br/authorization',
        'token_url':     'https://api.mercadolibre.com/oauth/token',
        'redirect_path': '/integrations/callback/mercado_livre',
        'scopes':        'read_orders write_orders read_items write_items read_questions write_questions manage_campaigns',
        'icon':          '🛒',
        'color':         '#ffe600',
    },
    'meta_ads': {
        'name':          'Meta Ads',
        'client_id':     os.environ.get('META_APP_ID',     'SEU_APP_ID_AQUI'),
        'client_secret': os.environ.get('META_APP_SECRET', 'SEU_SECRET_AQUI'),
        'auth_url':      'https://www.facebook.com/v18.0/dialog/oauth',
        'token_url':     'https://graph.facebook.com/v18.0/oauth/access_token',
        'redirect_path': '/integrations/callback/meta_ads',
        'scopes':        'ads_read,ads_management,business_management,pages_read_engagement',
        'icon':          '📘',
        'color':         '#1877f2',
    },
    'google_ads': {
        'name':          'Google Ads',
        'client_id':     os.environ.get('GOOGLE_CLIENT_ID',     'SEU_CLIENT_ID_AQUI'),
        'client_secret': os.environ.get('GOOGLE_CLIENT_SECRET', 'SEU_SECRET_AQUI'),
        'auth_url':      'https://accounts.google.com/o/oauth2/v2/auth',
        'token_url':     'https://oauth2.googleapis.com/token',
        'redirect_path': '/integrations/callback/google_ads',
        'scopes':        'https://www.googleapis.com/auth/adwords https://www.googleapis.com/auth/analytics.readonly',
        'icon':          '🔴',
        'color':         '#ea4335',
        'extra_params':  {'access_type': 'offline', 'prompt': 'consent'},
    },
    'tiktok_shop': {
        'name':          'TikTok Shop',
        'client_id':     os.environ.get('TIKTOK_APP_KEY',    'SEU_APP_KEY_AQUI'),
        'client_secret': os.environ.get('TIKTOK_APP_SECRET', 'SEU_SECRET_AQUI'),
        'auth_url':      'https://auth.tiktok-shops.com/oauth/authorize',
        'token_url':     'https://auth.tiktok-shops.com/api/v2/token/get',
        'redirect_path': '/integrations/callback/tiktok_shop',
        'scopes':        'order.read,product.read,product.write,shop.read,campaign.read,report.read',
        'icon':          '🎵',
        'color':         '#ff0050',
    },
    'tiktok_ads': {
        'name':          'TikTok Ads',
        'client_id':     os.environ.get('TIKTOK_ADS_APP_ID',     'SEU_APP_ID_AQUI'),
        'client_secret': os.environ.get('TIKTOK_ADS_APP_SECRET', 'SEU_SECRET_AQUI'),
        'auth_url':      'https://ads.tiktok.com/marketing_api/auth',
        'token_url':     'https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/',
        'redirect_path': '/integrations/callback/tiktok_ads',
        'scopes':        '',
        'icon':          '🎯',
        'color':         '#000000',
    },
    'google_analytics': {
        'name':          'Google Analytics 4',
        'client_id':     os.environ.get('GOOGLE_CLIENT_ID',     'SEU_CLIENT_ID_AQUI'),
        'client_secret': os.environ.get('GOOGLE_CLIENT_SECRET', 'SEU_SECRET_AQUI'),
        'auth_url':      'https://accounts.google.com/o/oauth2/v2/auth',
        'token_url':     'https://oauth2.googleapis.com/token',
        'redirect_path': '/integrations/callback/google_analytics',
        'scopes':        'https://www.googleapis.com/auth/analytics.readonly',
        'icon':          '📊',
        'color':         '#e37400',
        'extra_params':  {'access_type': 'offline', 'prompt': 'consent'},
    },
    'bling': {
        'name':          'Bling ERP',
        'client_id':     os.environ.get('BLING_CLIENT_ID',     'SEU_CLIENT_ID_AQUI'),
        'client_secret': os.environ.get('BLING_CLIENT_SECRET', 'SEU_SECRET_AQUI'),
        'auth_url':      'https://www.bling.com.br/OAuth2/Login',
        'token_url':     'https://www.bling.com.br/OAuth2/Token',
        'redirect_path': '/integrations/callback/bling',
        'scopes':        'pedidos+produtos+estoque+nfe+financeiro',
        'icon':          '💼',
        'color':         '#0066cc',
    },
}

# Plataformas que usam API Key (sem OAuth)
API_KEY_PLATFORMS = {
    'amazon': {
        'name': 'Amazon Seller',
        'icon': '📦',
        'color': '#ff9900',
        'fields': ['seller_id', 'marketplace_id', 'client_id', 'client_secret', 'refresh_token'],
    },
    'shopee': {
        'name': 'Shopee',
        'icon': '🧡',
        'color': '#ee4d2d',
        'fields': ['shop_id', 'partner_id', 'partner_key'],
    },
    'tiny_erp': {
        'name': 'Tiny ERP',
        'icon': '🗂️',
        'color': '#00a651',
        'fields': ['api_token'],
    },
    'melhor_envio': {
        'name': 'Melhor Envio',
        'icon': '🚚',
        'color': '#00b14f',
        'fields': ['api_token'],
    },
    'whatsapp_business': {
        'name': 'WhatsApp Business',
        'icon': '💬',
        'color': '#25d366',
        'fields': ['phone_id', 'wa_token', 'waba_id'],
    },
    'rdstation': {
        'name': 'RD Station',
        'icon': '📧',
        'color': '#3ab0e8',
        'fields': ['api_key'],
    },
    'magalu': {
        'name': 'Magalu',
        'icon': '💙',
        'color': '#0086ff',
        'fields': ['api_key', 'tenant_id'],
    },
}


def get_base_url(request_host=None):
    """Retorna a URL base do servidor atual."""
    base = os.environ.get('SELLVANCE_BASE_URL', '')
    if not base and request_host:
        # Auto-detecta em Replit
        if 'replit' in (request_host or ''):
            base = f'https://{request_host}'
        else:
            base = f'http://{request_host}'
    return base.rstrip('/')


def build_auth_url(platform, org_id, request_host=None):
    """Gera a URL de autorização OAuth para redirecionar o cliente."""
    app = OAUTH_APPS.get(platform)
    if not app:
        return None

    base_url  = get_base_url(request_host)
    redirect  = base_url + app['redirect_path']
    state     = f'{org_id}:{platform}'  # identificar o org no callback

    params = {
        'client_id':     app['client_id'],
        'redirect_uri':  redirect,
        'response_type': 'code',
        'scope':         app['scopes'],
        'state':         state,
    }

    # Parâmetros extras por plataforma (ex: Google precisa de access_type=offline)
    if 'extra_params' in app:
        params.update(app['extra_params'])

    return app['auth_url'] + '?' + urllib.parse.urlencode(params)


def exchange_code_for_token(platform, code, request_host=None):
    """Troca o authorization code pelo access_token + refresh_token."""
    app = OAUTH_APPS.get(platform)
    if not app:
        return None, 'Plataforma não encontrada'

    base_url = get_base_url(request_host)
    redirect = base_url + app['redirect_path']

    payload = urllib.parse.urlencode({
        'grant_type':    'authorization_code',
        'code':          code,
        'redirect_uri':  redirect,
        'client_id':     app['client_id'],
        'client_secret': app['client_secret'],
    }).encode()

    try:
        req = urllib.request.Request(
            app['token_url'],
            data=payload,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            return data, None
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        return None, f'Erro {e.code}: {body[:200]}'
    except Exception as ex:
        return None, str(ex)


def refresh_access_token(platform, refresh_token):
    """Renova o access_token usando o refresh_token salvo."""
    app = OAUTH_APPS.get(platform)
    if not app:
        return None, 'Plataforma não encontrada'

    payload = urllib.parse.urlencode({
        'grant_type':    'refresh_token',
        'refresh_token': refresh_token,
        'client_id':     app['client_id'],
        'client_secret': app['client_secret'],
    }).encode()

    try:
        req = urllib.request.Request(
            app['token_url'],
            data=payload,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read()), None
    except Exception as ex:
        return None, str(ex)


def save_integration(org_id, platform, token_data, account_info=None):
    """Salva/atualiza os tokens de uma integração no banco."""
    db = get_db()

    config = json.dumps({
        'access_token':  token_data.get('access_token', ''),
        'refresh_token': token_data.get('refresh_token', ''),
        'token_type':    token_data.get('token_type', 'Bearer'),
        'expires_in':    token_data.get('expires_in', 3600),
        'scope':         token_data.get('scope', ''),
        # Campos extras por plataforma
        'user_id':       token_data.get('user_id', ''),
        'seller_id':     token_data.get('seller_id', account_info.get('seller_id', '') if account_info else ''),
    })

    account_id   = (account_info or {}).get('id', token_data.get('user_id', ''))
    account_name = (account_info or {}).get('name', token_data.get('nickname', ''))

    existing = db.execute(
        'SELECT id FROM api_integrations WHERE org_id=? AND platform=?',
        (org_id, platform)
    ).fetchone()

    if existing:
        db.execute('''
            UPDATE api_integrations
            SET status='connected', account_id=?, account_name=?, config_json=?, last_sync=datetime('now')
            WHERE org_id=? AND platform=?
        ''', (str(account_id), account_name, config, org_id, platform))
    else:
        db.execute('''
            INSERT INTO api_integrations (org_id, platform, status, account_id, account_name, config_json, last_sync)
            VALUES (?, ?, 'connected', ?, ?, ?, datetime('now'))
        ''', (org_id, platform, str(account_id), account_name, config))

    db.commit()
    db.close()


def save_api_key_integration(org_id, platform, fields):
    """Salva integração baseada em API Key/tokens manuais."""
    db = get_db()
    config = json.dumps(fields)

    existing = db.execute(
        'SELECT id FROM api_integrations WHERE org_id=? AND platform=?',
        (org_id, platform)
    ).fetchone()

    account_id   = fields.get('seller_id') or fields.get('shop_id') or fields.get('api_key', '')[:8]
    account_name = fields.get('account_name', '')

    if existing:
        db.execute('''
            UPDATE api_integrations
            SET status='connected', account_id=?, account_name=?, config_json=?, last_sync=datetime('now')
            WHERE org_id=? AND platform=?
        ''', (account_id, account_name, config, org_id, platform))
    else:
        db.execute('''
            INSERT INTO api_integrations (org_id, platform, status, account_id, account_name, config_json, last_sync)
            VALUES (?, ?, 'connected', ?, ?, ?, datetime('now'))
        ''', (org_id, platform, account_id, account_name, config))

    db.commit()
    db.close()


def get_integration(org_id, platform):
    """Retorna os dados de integração de uma org para uma plataforma."""
    db = get_db()
    row = db.execute(
        'SELECT * FROM api_integrations WHERE org_id=? AND platform=?',
        (org_id, platform)
    ).fetchone()
    db.close()
    if not row:
        return None
    result = dict(row)
    try:
        result['config'] = json.loads(result.get('config_json', '{}'))
    except Exception:
        result['config'] = {}
    return result


def get_all_integrations(org_id):
    """Retorna todas as integrações de uma org."""
    db = get_db()
    rows = db.execute(
        'SELECT * FROM api_integrations WHERE org_id=? ORDER BY platform',
        (org_id,)
    ).fetchall()
    db.close()
    result = {}
    for row in rows:
        r = dict(row)
        try:
            r['config'] = json.loads(r.get('config_json', '{}'))
        except Exception:
            r['config'] = {}
        result[r['platform']] = r
    return result


def disconnect_integration(org_id, platform):
    """Desconecta uma integração, apagando os tokens."""
    db = get_db()
    db.execute('''
        UPDATE api_integrations
        SET status='disconnected', account_id=NULL, account_name=NULL,
            config_json='{}', last_sync=NULL
        WHERE org_id=? AND platform=?
    ''', (org_id, platform))
    db.commit()
    db.close()


def is_app_configured(platform):
    """Verifica se as credenciais do app foram configuradas no servidor."""
    app = OAUTH_APPS.get(platform)
    if not app:
        return False
    return 'SEU_' not in app['client_id']


# ── HELPERS DE FETCH POR PLATAFORMA ──────────────────────────────────────────

def fetch_ml_account_info(access_token):
    """Busca informações da conta ML do cliente."""
    try:
        req = urllib.request.Request(
            'https://api.mercadolibre.com/users/me',
            headers={'Authorization': f'Bearer {access_token}'}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return {'id': data.get('id'), 'name': data.get('nickname', data.get('first_name', ''))}
    except Exception:
        return {}


def fetch_meta_account_info(access_token):
    """Busca informações da conta Meta do cliente."""
    try:
        url = f'https://graph.facebook.com/v18.0/me?fields=id,name&access_token={access_token}'
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
            return {'id': data.get('id'), 'name': data.get('name', '')}
    except Exception:
        return {}


def fetch_google_account_info(access_token):
    """Busca informações da conta Google do cliente."""
    try:
        req = urllib.request.Request(
            'https://www.googleapis.com/oauth2/v2/userinfo',
            headers={'Authorization': f'Bearer {access_token}'}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return {'id': data.get('id'), 'name': data.get('name', data.get('email', ''))}
    except Exception:
        return {}
