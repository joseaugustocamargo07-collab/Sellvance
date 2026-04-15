# health_monitor.py — Health checks e rollback automatico
# Expoe /healthz (para Railway) e /healthz/deep (checagem profunda).
# Monitora error rate e pode disparar rollback de feature flags.

import time
from flask import jsonify
from database import get_db


# Threshold de erro: se > 5% em 5min, considerar unhealthy
ERROR_RATE_THRESHOLD = 0.05
WINDOW_MINUTES = 5


def register_routes(app):
    """Registra as rotas de health check no Flask app."""

    @app.route('/healthz')
    def healthz():
        """Health check simples para Railway/load balancers."""
        return jsonify({'status': 'ok', 'ts': int(time.time())}), 200

    @app.route('/healthz/deep')
    def healthz_deep():
        """
        Health check profundo: verifica DB, calcula error rate,
        retorna status detalhado.
        """
        result = {
            'status': 'ok',
            'ts': int(time.time()),
            'checks': {}
        }

        # Check 1: Database reachable
        try:
            db = get_db()
            db.execute('SELECT 1').fetchone()
            db.close()
            result['checks']['database'] = 'ok'
        except Exception as e:
            result['checks']['database'] = f'error: {str(e)[:100]}'
            result['status'] = 'degraded'

        # Check 2: Error rate nos ultimos 5min
        try:
            db = get_db()
            total = db.execute(
                '''SELECT COUNT(*) as c FROM events_log
                   WHERE event_type = 'request'
                   AND created_at > datetime('now', ?)''',
                (f'-{WINDOW_MINUTES} minutes',)
            ).fetchone()
            errors = db.execute(
                '''SELECT COUNT(*) as c FROM events_log
                   WHERE event_type = 'request'
                   AND status_code >= 500
                   AND created_at > datetime('now', ?)''',
                (f'-{WINDOW_MINUTES} minutes',)
            ).fetchone()
            db.close()
            total_count = total['c'] if total else 0
            error_count = errors['c'] if errors else 0
            rate = (error_count / total_count) if total_count > 0 else 0
            result['checks']['error_rate'] = {
                'total': total_count,
                'errors': error_count,
                'rate': round(rate, 4),
                'threshold': ERROR_RATE_THRESHOLD,
                'ok': rate <= ERROR_RATE_THRESHOLD
            }
            if rate > ERROR_RATE_THRESHOLD:
                result['status'] = 'degraded'
        except Exception as e:
            result['checks']['error_rate'] = f'error: {str(e)[:100]}'

        # Check 3: Sincronizacoes pendentes
        try:
            db = get_db()
            stuck = db.execute(
                '''SELECT COUNT(*) as c FROM api_integrations
                   WHERE status = 'connected'
                   AND (last_sync IS NULL OR last_sync < datetime('now', '-6 hours'))'''
            ).fetchone()
            db.close()
            result['checks']['stale_syncs'] = stuck['c'] if stuck else 0
        except Exception:
            result['checks']['stale_syncs'] = 'unknown'

        return jsonify(result), 200 if result['status'] == 'ok' else 503


def check_and_rollback(feature_name):
    """
    Verifica se uma feature esta causando erros.
    Se sim, dispara rollback automatico.
    Usado pelo auto_insights.
    """
    try:
        db = get_db()
        errors = db.execute(
            '''SELECT COUNT(*) as c FROM events_log
               WHERE event_type = 'perf'
               AND event_name = ?
               AND json_extract(metadata, '$.ok') = 0
               AND created_at > datetime('now', '-15 minutes')''',
            (feature_name,)
        ).fetchone()
        total = db.execute(
            '''SELECT COUNT(*) as c FROM events_log
               WHERE event_type = 'perf'
               AND event_name = ?
               AND created_at > datetime('now', '-15 minutes')''',
            (feature_name,)
        ).fetchone()
        db.close()

        e = errors['c'] if errors else 0
        t = total['c'] if total else 0
        if t < 10:
            return False  # amostra pequena demais
        rate = e / t
        if rate > 0.10:  # 10% de erro -> rollback
            from feature_flags import rollback as flag_rollback
            flag_rollback(feature_name)
            return True
    except Exception:
        pass
    return False
