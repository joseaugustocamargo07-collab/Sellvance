from flask import Flask, render_template, request, session, redirect, url_for, jsonify
from database import get_db
from auth import login_required, verify_password, hash_password
from traffic_ai import analyze_all, calc_metrics, score_campaign
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'sellvance-secret-2026-change-in-prod')

# Inicializa banco lazy (na primeira requisição) para não atrasar o startup
_db_ready = False

@app.before_request
def ensure_db_ready():
    global _db_ready
    if not _db_ready:
        _db_ready = True
        from database import init_db, migrate_db
        init_db()
        migrate_db()