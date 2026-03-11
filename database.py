import sqlite3
import os
from auth import hash_password

# Railway usa /data como volume persistente; localmente usa o diretório atual
DATA_DIR = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(DATA_DIR, 'sellvance.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn

def init_db():
    if os.path.exists(DB_PATH):
        return  # já inicializado

    db = get_db()

    db.executescript('''
        CREATE TABLE IF NOT EXISTS organizations (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT NOT NULL,
            plan     TEXT DEFAULT 'growth'
        );

        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id        INTEGER REFERENCES organizations(id),
            org_name      TEXT,
            name          TEXT NOT NULL,
            email         TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at    TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS contacts (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id        INTEGER NOT NULL,
            name          TEXT NOT NULL,
            email         TEXT NOT NULL,
            phone         TEXT,
            source        TEXT DEFAULT 'manual',
            rfm_segment   TEXT DEFAULT 'new',
            ltv           REAL DEFAULT 0,
            total_orders  INTEGER DEFAULT 0,
            last_order_at TEXT,
            wa_opt_in     INTEGER DEFAULT 0,
            email_opt_in  INTEGER DEFAULT 1,
            created_at    TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS orders (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id      INTEGER NOT NULL,
            contact_id  INTEGER,
            marketplace TEXT NOT NULL,
            external_id TEXT,
            status      TEXT DEFAULT 'delivered',
            gmv         REAL DEFAULT 0,
            revenue     REAL DEFAULT 0,
            cost        REAL DEFAULT 0,
            channel     TEXT DEFAULT 'organic',
            ordered_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS ad_campaigns (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id               INTEGER NOT NULL,
            platform             TEXT NOT NULL,
            external_campaign_id TEXT,
            name                 TEXT NOT NULL,
            objective            TEXT DEFAULT 'conversao',
            audience             TEXT DEFAULT 'broad',
            spend                REAL DEFAULT 0,
            budget_daily         REAL DEFAULT 0,
            revenue              REAL DEFAULT 0,
            impressions          INTEGER DEFAULT 0,
            clicks               INTEGER DEFAULT 0,
            conversions          INTEGER DEFAULT 0,
            leads                INTEGER DEFAULT 0,
            reach                INTEGER DEFAULT 0,
            video_views          INTEGER DEFAULT 0,
            status               TEXT DEFAULT 'active',
            paused_by_ai         INTEGER DEFAULT 0,
            ai_note              TEXT DEFAULT '',
            date                 TEXT DEFAULT (date('now'))
        );

        CREATE TABLE IF NOT EXISTS campaign_daily (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER REFERENCES ad_campaigns(id),
            org_id      INTEGER NOT NULL,
            date        TEXT NOT NULL,
            spend       REAL DEFAULT 0,
            revenue     REAL DEFAULT 0,
            clicks      INTEGER DEFAULT 0,
            impressions INTEGER DEFAULT 0,
            conversions INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS api_integrations (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id       INTEGER NOT NULL,
            platform     TEXT NOT NULL,
            status       TEXT DEFAULT 'disconnected',
            account_id   TEXT,
            account_name TEXT,
            last_sync    TEXT,
            config_json  TEXT DEFAULT '{}',
            UNIQUE(org_id, platform)
        );
    ''')
