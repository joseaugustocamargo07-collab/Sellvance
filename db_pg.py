"""
db_pg.py  —  PostgreSQL compatibility wrapper for Sellvance.

Makes psycopg2 connections behave like sqlite3 connections used throughout
the app.  When DATABASE_URL is set (Railway PostgreSQL plugin sets this
automatically), get_db() returns a PGConn instance instead of sqlite3.
"""
import os, re


# ─── SQL translation helpers ─────────────────────────────────────────────────

def _translate_query(sql):
    """Translate SQLite-dialect SQL to PostgreSQL SQL."""
    sql = sql.replace("?", "%s")
    sql = re.sub(r"datetime\('now'\)", "NOW()", sql, flags=re.IGNORECASE)
    sql = re.sub(r"date\('now'\)", "CURRENT_DATE", sql, flags=re.IGNORECASE)
    # datetime('now', %s || ' days')  →  (NOW() + (%s || ' days')::interval)
    sql = re.sub(
        r"datetime\('now',\s*(%s)\s*\|\|\s*' days'\)",
        r"(NOW() + (\1 || ' days')::interval)",
        sql, flags=re.IGNORECASE,
    )
    # date(col) >= date(%s)  →  col::date >= %s::date
    sql = re.sub(
        r"date\((\w+)\)\s*(>=|<=|>|<|=)\s*date\((%s)\)",
        r"\1::date \2 \3::date",
        sql, flags=re.IGNORECASE,
    )
    sql = re.sub(r"last_insert_rowid\s*\(\s*\)", "lastval()", sql, flags=re.IGNORECASE)
    sql = re.sub(r"SELECT\s+changes\s*\(\s*\)", "SELECT 0", sql, flags=re.IGNORECASE)
    if re.match(r"^\s*PRAGMA", sql, re.IGNORECASE):
        return "SELECT 1"
    sql = re.sub(
        r"SELECT\s+name\s+FROM\s+sqlite_master\s+WHERE\s+type\s*=\s*'table'",
        "SELECT table_name AS name FROM information_schema.tables "
        "WHERE table_schema='public' AND table_type='BASE TABLE'",
        sql, flags=re.IGNORECASE,
    )
    if re.match(r"^\s*INSERT\s+OR\s+IGNORE\s+INTO", sql, re.IGNORECASE):
        sql = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", sql, flags=re.IGNORECASE)
        sql = sql.rstrip(";").rstrip() + " ON CONFLICT DO NOTHING"
    return sql


def _translate_ddl(sql):
    """Translate CREATE TABLE DDL from SQLite to PostgreSQL."""
    sql = re.sub(
        r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
        "SERIAL PRIMARY KEY", sql, flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\bTEXT\s+DEFAULT\s+\(datetime\('now'\)\)",
        "TIMESTAMPTZ DEFAULT NOW()", sql, flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\bTEXT\s+DEFAULT\s+\(date\('now'\)\)",
        "DATE DEFAULT CURRENT_DATE", sql, flags=re.IGNORECASE,
    )
    return sql


def _split_statements(script):
    """Split SQL script into individual statements, respecting string literals."""
    statements, current, in_string, sc = [], [], False, None
    for ch in script:
        if in_string:
            current.append(ch)
            if ch == sc:
                in_string = False
        elif ch in ("'", '"'):
            in_string, sc = True, ch
            current.append(ch)
        elif ch == ";":
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(ch)
    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)
    return statements


# ─── Row wrapper ─────────────────────────────────────────────────────────────

class DictRow(dict):
    """Dict with integer-index support (mimics sqlite3.Row)."""
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


# ─── Cursor wrapper ──────────────────────────────────────────────────────────

class PGCursor:
    def __init__(self, cur):
        self._cur = cur

    def fetchone(self):
        row = self._cur.fetchone()
        return DictRow(row) if row is not None else None

    def fetchall(self):
        return [DictRow(r) for r in self._cur.fetchall()]

    def __iter__(self):
        for row in self._cur:
            yield DictRow(row)


# ─── Connection wrapper ──────────────────────────────────────────────────────

class PGConn:
    """
    Wraps a psycopg2 connection to behave like sqlite3.Connection.
    SQL dialect differences are translated transparently.
    """

    def __init__(self, dsn):
        import psycopg2
        import psycopg2.extras as _x
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        self._extras = _x

    def _cur(self):
        return self._conn.cursor(cursor_factory=self._extras.RealDictCursor)

    def execute(self, sql, params=()):
        translated = _translate_query(sql)
        cur = self._cur()
        try:
            cur.execute(translated, params if params else None)
        except Exception:
            try:
                self._conn.rollback()
            except Exception:
                pass
            raise
        return PGCursor(cur)

    def executescript(self, script):
        """Multi-statement DDL/DML execution (sqlite3 extension, emulated)."""
        import psycopg2
        for stmt in _split_statements(script):
            stmt = stmt.strip()
            if not stmt:
                continue
            if re.match(r"^\s*(CREATE|DROP|ALTER)", stmt, re.IGNORECASE):
                stmt = _translate_ddl(stmt)
            stmt = _translate_query(stmt)
            cur = self._cur()
            try:
                cur.execute(stmt)
                self._conn.commit()
            except (psycopg2.errors.DuplicateTable,
                    psycopg2.errors.DuplicateObject):
                self._conn.rollback()   # already exists — fine
            except Exception as exc:
                print(f"[PGConn.executescript] {type(exc).__name__}: {exc} | {stmt[:80]}")
                try:
                    self._conn.rollback()
                except Exception:
                    pass

    def commit(self):
        self._conn.commit()

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


# ─── Factory ─────────────────────────────────────────────────────────────────

def connect_pg():
    """Return a PGConn using DATABASE_URL."""
    url = os.environ.get("DATABASE_URL", "")
    url = re.sub(r"^postgres://", "postgresql://", url)
    return PGConn(url)


def is_pg():
    """True when DATABASE_URL is set (PostgreSQL mode)."""
    return bool(os.environ.get("DATABASE_URL", ""))
