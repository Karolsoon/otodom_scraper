import sqlite3

from src.utils.log_util import get_logger
import config


log = get_logger(__name__, 30, True, True)

DB_NAME = config.DB_NAME


def connect() -> sqlite3.Connection:
    """
    Connect to the SQLite database.
    """
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def create_tables():
    """
    Create the audit_logs table and urls table if it doesn't exist.
    """
    conn = connect()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_id TEST NOT NULL,
            url TEXT NOT NULL,
            status INT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            expired_at DATETIME DEFAULT NULL
        )
    ''')
    conn.commit()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_id TEXT NOT NULL,
            status_code TEXT NOT NULL,
            html_file_path TEXT NOT NULL,
            error_message TEXT DEFAULT NULL,
            status INT NOT NULL,
            visited_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def get_active_urls() -> list[str]:
    """
    Get all active URLs from the urls table.
    """
    conn = connect()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT url FROM urls WHERE status = 1
    ''')
    rows = cursor.fetchall()
    conn.close()
    return [row['url'] for row in rows]


def insert_url_if_not_exists(id4: str, url: str, timestamp: str) -> int:
    """
    Insert a URL into the urls table.
    """
    log.debug(f'{id4}')
    conn = connect()
    cursor = conn.cursor()
    try:
        r = cursor.execute('''
            INSERT INTO urls (url_id, url, status, created_at, updated_at)
            SELECT 
                ? as url_id,
                ? as url,
                ? as status,
                ? as created_at,
                ? as updated_at
            WHERE NOT EXISTS (
                SELECT 1 FROM urls WHERE url_id = ? and status = 1)
            RETURNING *;
        ''', (id4, url, 1, timestamp, timestamp, id4))
        a = r.fetchall()
        conn.commit()
        return len(a)
    except sqlite3.IntegrityError as ex:
        log.error(f'FAILED {id4}')
        log.exception(ex)
        return 0
    finally:
        conn.close()

def update_url(
        id4: str,
        updated_at: str,
        expited_at: str|None=None,
        status: int=1) -> int:
    """
    Update the updated_at of a URL
    which has status = 1
    """
    conn = connect()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE urls
            SET status = ?, updated_at = ?, expired_at = ?
            WHERE url_id = ? and status = 1
        ''', (status, updated_at, expited_at, id4))
        conn.commit()
        log.debug(f'OK {id4}')
        return 1
    except sqlite3.IntegrityError as ex:
        log.error(f'FAILED {id4}')
        log.exception(ex)
        return 0
    finally:
        conn.close()


def insert_audit_log(
    id4: str,
    status_code: str,
    html_file_path: str,
    error_message: str,
    visited_at: str,
    status: int=1
) -> bool:
    """
    Insert an audit log into the audit_logs table.
    """
    conn = connect()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO audit_logs (url_id, status_code, html_file_path, error_message, status, visited_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (id4, status_code, html_file_path, error_message, status, visited_at))
        conn.commit()
        return True
    except sqlite3.IntegrityError as ex:
        log.error(f'FAILED {id4}')
        log.exception(ex)
        return False
    finally:
        conn.close()
