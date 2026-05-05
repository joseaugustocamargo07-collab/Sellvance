import hashlib
import hmac
import bcrypt
from functools import wraps
from flask import session, redirect, url_for

# Hashes em bcrypt comecam com $2a$, $2b$ ou $2y$.
_BCRYPT_PREFIX = ('$2a$', '$2b$', '$2y$')


def hash_password(password: str) -> str:
    """Gera hash bcrypt (cost 12) de uma senha em texto plano."""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(rounds=12)).decode('utf-8')


def verify_password(password: str, stored: str) -> bool:
    """
    Valida senha contra hash armazenado.
    Aceita bcrypt (formato novo) e SHA-256+salt (formato legado, pre-migracao).
    """
    if not stored:
        return False
    if stored.startswith(_BCRYPT_PREFIX):
        try:
            return bcrypt.checkpw(password.encode('utf-8'), stored.encode('utf-8'))
        except Exception:
            return False
    # Legado: "salt:hash"
    try:
        salt, hashed = stored.split(':', 1)
    except ValueError:
        return False
    computed = hashlib.sha256((password + salt).encode()).hexdigest()
    return hmac.compare_digest(computed, hashed)


def needs_rehash(stored: str) -> bool:
    """True se o hash armazenado esta em formato legado e deve ser re-hasheado."""
    return bool(stored) and not stored.startswith(_BCRYPT_PREFIX)


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated
