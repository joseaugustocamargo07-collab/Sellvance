# telemetry.py — Coleta silenciosa de eventos de uso
# Parte do sistema de auto-melhoria do Sellvance.
# Registra ações do usuário, performance e erros para que o auto_insights
# possa analisar padrões e sugerir otimizações automaticamente.

import json
import time
from functools import wraps
from flask import request, session, g
from database import get_db


def ensure_tables():
    """Cria tabelas de telemetria se ainda nao existirem (idempotente)."""
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS events_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id       INTEGER,
            user_id      INTEGER,
            event_type   TEXT NOT NULL,
            event_name   TEXT NOT NULL,
            page         TEXT,
            duration_ms  INTEGER,
            status_code  INTEGER,
            metadata     TEXT DEFAULT '{}',
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_events_org_date
            ON events_log(org_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_events_type
            ON events_log(event_type, event_name);

        CREATE TABLE IF NOT EXISTS performance_metrics (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            route        TEXT NOT NULL,
            method       TEXT NOT NULL,
            p50_ms       REAL DEFAULT 0,
            p95_ms       REAL DEFAULT 0,
            p99_ms       REAL DEFAULT 0,
            error_rate   REAL DEFAULT 0,
            sample_count INTEGER DEFAULT 0,
            window_start TEXT,
            window_end   TEXT,
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS error_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id       INTEGER,
            route        TEXT,
            error_type   TEXT,
            error_msg    TEXT,
            traceback    TEXT,
            created_at   TEXT DEFAULT (datetime('now'))
        );
    ''')
    db.commit()
    db.close()


def track(event_type, event_name, **metadata):
    """
    Registra um evento de forma assincrona e nao-bloqueante.
    Nunca deve lancar excecao que afete o fluxo do usuario.

    Uso:
        track('action', 'campaign_paused', campaign_id=123)
        track('view', 'dashboard')
    """
    try:
        db = get_db()
        org_id = session.get('org_id') if session else None
        user_id = session.get('user_id') if session else None
        page = request.path if request else None
        db.execute(
            '''INSERT INTO events_log
               (org_id, user_id, event_type, event_name, page, metadata)
               VALUES (?, ?, ?, ?, ?, ?)''',
            (org_id, user_id, event_type, event_name, page, json.dumps(metadata))
        )
        db.commit()
        db.close()
    except Exception:
        pass  # telemetria NUNCA quebra a aplicacao


def track_error(error_type, error_msg, traceback=None, route=None):
    """Registra um erro para analise posterior."""
    try:
        db = get_db()
        org_id = session.get('org_id') if session else None
        db.execute(
            '''INSERT INTO error_log
               (org_id, route, error_type, error_msg, traceback)
               VALUES (?, ?, ?, ?, ?)''',
            (org_id, route or (request.path if request else None),
             error_type, str(error_msg)[:500], str(traceback)[:2000] if traceback else None)
        )
        db.commit()
        db.close()
    except Exception:
        pass


def timed(event_name):
    """
    Decorator que mede latencia de uma funcao e registra em events_log.

    @timed('pricing_analysis')
    def analyze_pricing(): ...
    """
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            start = time.time()
            try:
                result = fn(*args, **kwargs)
                duration_ms = int((time.time() - start) * 1000)
                track('perf', event_name, duration_ms=duration_ms, ok=True)
                return result
            except Exception as e:
                duration_ms = int((time.time() - start) * 1000)
                track('perf', event_name, duration_ms=duration_ms, ok=False, error=str(e)[:200])
                raise
        return wrapped
    return decorator


def register_request_hooks(app):
    """
    Hooks do Flask: mede latencia de cada request automaticamente.
    Chame uma vez no bootstrap: telemetry.register_request_hooks(app)
    """
    @app.before_request
    def _telemetry_start():
        g._telemetry_t0 = time.time()

    @app.after_request
    def _telemetry_end(response):
        try:
            t0 = getattr(g, '_telemetry_t0', None)
            if t0 is None:
                return response
            duration_ms = int((time.time() - t0) * 1000)
            # So registra requests relevantes (evita ruido de assets)
            path = request.path
            if (path.startswith('/static/') or path.startswith('/favicon')
                    or path == '/healthz'):
                return response
            db = get_db()
            org_id = session.get('org_id') if session else None
            user_id = session.get('user_id') if session else None
            db.execute(
                '''INSERT INTO events_log
                   (org_id, user_id, event_type, event_name, page, duration_ms, status_code)
                   VALUES (?, ?, 'request', ?, ?, ?, ?)''',
                (org_id, user_id, request.method, path, duration_ms, response.status_code)
            )
            db.commit()
            db.close()
        except Exception:
            pass
        return response


def get_recent_events(org_id=None, limit=100, event_type=None):
    """Retorna eventos recentes (para dashboard admin)."""
    db = get_db()
    sql = 'SELECT * FROM events_log WHERE 1=1'
    params = []
    if org_id is not None:
        sql += ' AND org_id = ?'
        params.append(org_id)
    if event_type:
        sql += ' AND event_type = ?'
        params.append(event_type)
    sql += ' ORDER BY id DESC LIMIT ?'
    params.append(limit)
    rows = [dict(r) for r in db.execute(sql, params).fetchall()]
    db.close()
    return rows


def cleanup_old_events(days=30):
    """Remove eventos mais antigos que N dias (chamar via cron)."""
    db = get_db()
    db.execute(
        "DELETE FROM events_log WHERE created_at < datetime('now', ?)",
        (f'-{days} days',)
    )
    db.commit()
    db.close()
