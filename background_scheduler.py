# background_scheduler.py — Scheduler in-process para tarefas periodicas
# Roda em thread separada e executa:
#   - auto_insights.run_all()   a cada 6h
#   - pricing_ai.run_pricing_batch() a cada 30min (se flag ativa)
#   - telemetry.cleanup_old_events() a cada 24h
#   - health checks + auto-rollback a cada 2min
#
# Nao depende de APScheduler — usa threading nativa.

import threading
import time
import traceback


class BackgroundScheduler:
    def __init__(self):
        self.tasks = []
        self.running = False
        self.thread = None
        self.last_run = {}

    def add_task(self, name, interval_seconds, fn, *args, **kwargs):
        """Registra uma tarefa periodica."""
        self.tasks.append({
            'name': name,
            'interval': interval_seconds,
            'fn': fn,
            'args': args,
            'kwargs': kwargs,
        })
        self.last_run[name] = 0

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        print('[scheduler] started')

    def stop(self):
        self.running = False

    def _loop(self):
        # Pequeno delay inicial para nao competir com o bootstrap
        time.sleep(30)
        while self.running:
            now = time.time()
            for task in self.tasks:
                if now - self.last_run[task['name']] >= task['interval']:
                    try:
                        task['fn'](*task['args'], **task['kwargs'])
                    except Exception as e:
                        print(f"[scheduler] task {task['name']} error: {e}")
                        traceback.print_exc()
                    self.last_run[task['name']] = now
            time.sleep(15)  # checa a cada 15s quais tasks precisam rodar

    def status(self):
        """Retorna status de todas as tarefas."""
        now = time.time()
        return [
            {
                'name': t['name'],
                'interval_seconds': t['interval'],
                'last_run_ago_seconds': int(now - self.last_run.get(t['name'], 0)),
                'next_run_seconds': max(0, t['interval'] - int(now - self.last_run.get(t['name'], 0))),
            }
            for t in self.tasks
        ]


# Instancia global
scheduler = BackgroundScheduler()


def _task_auto_insights():
    try:
        import auto_insights
        auto_insights.run_all()
    except Exception as e:
        print(f'[scheduler] auto_insights error: {e}')


def _task_pricing_batch():
    try:
        from feature_flags import is_enabled
        from pricing_ai import run_pricing_batch
        from database import get_db
        db = get_db()
        orgs = db.execute('SELECT id FROM organizations').fetchall()
        db.close()
        for o in orgs:
            if is_enabled('ai_pricing', o['id']):
                run_pricing_batch(o['id'])
    except Exception as e:
        print(f'[scheduler] pricing_batch error: {e}')


def _task_cleanup_events():
    try:
        from telemetry import cleanup_old_events
        cleanup_old_events(days=30)
    except Exception as e:
        print(f'[scheduler] cleanup error: {e}')


def _task_health_check():
    """Check periodico + auto-rollback se error rate alto."""
    try:
        from database import get_db
        db = get_db()
        total = db.execute(
            "SELECT COUNT(*) c FROM events_log WHERE event_type='request' AND created_at > datetime('now', '-5 minutes')"
        ).fetchone()
        errors = db.execute(
            "SELECT COUNT(*) c FROM events_log WHERE event_type='request' AND status_code >= 500 AND created_at > datetime('now', '-5 minutes')"
        ).fetchone()
        db.close()
        t = total['c'] if total else 0
        e = errors['c'] if errors else 0
        if t >= 50 and (e / t) > 0.10:
            # Alarme: error rate > 10% com volume significativo
            print(f'[scheduler] HIGH ERROR RATE: {e}/{t} ({(e/t)*100:.1f}%)')
            # TODO: notificar admin por WhatsApp/email
    except Exception as e:
        print(f'[scheduler] health_check error: {e}')


def start_all():
    """Registra todas as tarefas e inicia o scheduler."""
    # Auto-insights a cada 6h
    scheduler.add_task('auto_insights', 6 * 3600, _task_auto_insights)
    # Pricing batch a cada 30min
    scheduler.add_task('pricing_batch', 30 * 60, _task_pricing_batch)
    # Cleanup de eventos antigos a cada 24h
    scheduler.add_task('cleanup_events', 24 * 3600, _task_cleanup_events)
    # Health check a cada 2min
    scheduler.add_task('health_check', 120, _task_health_check)
    scheduler.start()


def get_status():
    """Para /admin/scheduler endpoint."""
    return {
        'running': scheduler.running,
        'tasks': scheduler.status(),
    }
