import sqlite3

from src.database import queries
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


def execute_with_return(query: str, data: tuple|None = None) -> list[dict[str, str]]:
    """
    Execute a query and return the result.
    """
    conn = connect()
    cursor = conn.cursor()
    if data:
        cursor.execute(query, data)
    else:
        cursor.execute(query)
    rows = cursor.fetchall()
    conn.commit()
    conn.close()
    return [{k: row[k] for k in row.keys()} for row in rows]

def execute_no_return(query: str, data: tuple|None = None) -> None:
    """
    Execute a non-returning DDL query
    using the provided data tuple - if provided
    """
    conn = connect()
    cursor = conn.cursor()
    if data:
        cursor.execute(query, data)
    else:
        cursor.execute(query)
    conn.commit()
    conn.close()

def create_tables():
    """
    Create the audit_logs table and urls table if it doesn't exist.
    """
    conn = connect()
    cursor = conn.cursor()
    try:
        cursor.execute(queries.Urls.DDL)
        conn.commit()
        cursor.execute(queries.Audit_Logs.DDL)
        conn.commit()
        cursor.execute(queries.Offers.DDL)
        conn.commit()
        cursor.execute(queries.Favorites.DDL)
        conn.commit()
        cursor.execute(queries.Normalized_Addresses.DDL)
        conn.commit()
        cursor.execute(queries.Run_Logs.DDL)
        conn.commit()
        conn.close()
    except sqlite3.Error as exc:
        conn.rollback()
        conn.close()
        raise exc from exc


def get_active_urls(entity: str|None = None) -> list[str]:
    """
    Get all active URLs from the urls table.
    """
    entity_clause = '' if not entity else f"AND entity = '{entity}'"
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(f'''
        SELECT url
        FROM urls
        WHERE status = 1
        {entity_clause}
    ''')
    rows = cursor.fetchall()
    conn.close()
    return [row['url'] for row in rows]


def get(table: str, columns: list[str], filters: list[tuple[str, str|int]]|None = None) -> list[dict[str, str]]:
    """
    Get all active records from a table.
    """
    filter_clause = _get_filter_clause(filters)
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(f'''
        SELECT {', '.join(columns)}
        FROM {table}
        WHERE status IN (1,2)
        {filter_clause}
    ''')
    rows = cursor.fetchall()
    conn.close()
    return [{k: row[k] for k in row.keys()} for row in rows]


def _get_filter_clause(filters: list[tuple[str, str|int]]|None) -> str:
    """
    Get the filter clause for the SQL query.
    """
    if filters is None:
        return ''
    filter_clause = 'AND ' + ' AND '.join([f"{k} = '{v}'" for k, v in filters])
    return filter_clause


def get_latest_id4_status(url_id: str) -> int:
    """
    Returns the int status of a url_id from the urls table.
    1  - active
    2  - historical/expired
    -1 - not found in db
    """
    conn = connect()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT u.status
        FROM urls u
        WHERE url_id = ?
        ORDER BY id DESC
        LIMIT 1
    ''', (url_id,))
    status = cursor.fetchone()
    conn.close()
    if len(status):
        return list(status)[0]
    return -1


def insert_url_if_not_exists(id4: str, url: str, timestamp: str, house_or_flat: str) -> int:
    """
    Insert a URL into the urls table.

    house_or_flat is either 'houses' or 'flats'
    """
    log.debug(f'{id4}')
    conn = connect()
    cursor = conn.cursor()
    try:
        r = cursor.execute('''
            INSERT INTO urls (url_id, url, entity, status, created_at, updated_at)
            SELECT 
                ? as url_id,
                ? as url,
                ? as entity,
                ? as status,
                ? as created_at,
                ? as updated_at
            WHERE NOT EXISTS (
                SELECT 1 FROM urls WHERE url_id = ? and status = 1)
            RETURNING *;
        ''', (id4, url, house_or_flat, 1, timestamp, timestamp, id4))
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
        expired_at: str|None=None,
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
        ''', (status, updated_at, expired_at, id4))
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


def upsert_offer(id4: str, entity: str, data: dict[str, str|int|None]) -> None:
    conn = connect()
    cursor = conn.cursor()
    data['url_id'] = id4
    data['status'] = 1
    data['entity'] = entity

    if get('offers', ['id'], [('url_id', id4)]):
        log.debug(f'SET HISTORICAL {id4}')
        q = 'UPDATE offers SET status = 2 WHERE url_id = ?'
        cursor.execute(q, (id4,))
        conn.commit()

    insert_query = """
    INSERT INTO offers (
        url_id, status, entity, city, postal_code, street, price, area, price_per_m2, floors, floor, rooms,
        build_year, building_type, building_material, rent, windows, land_area, construction_status, market, posted_by,
        coordinates_lat_lon, informacje_dodatkowe_json, media_json, ogrodzenie_json, dojazd_json, ogrzewanie_json,
        okolica_json, zabezpieczenia_json, wyposazenie_json, ground_plan, description, contact, owner
    ) VALUES (
        :url_id, :status, :entity, :city, :postal_code, :street, :price, :area, :price_per_m2, :floors, :floor, :rooms,
        :build_year, :building_type, :building_material, :rent, :windows, :land_area, :construction_status, :market, :posted_by,
        :coordinates_lat_lon, :informacje_dodatkowe_json, :media_json, :ogrodzenie_json, :dojazd_json,
        :ogrzewanie_json, :okolica_json, :zabezpieczenia_json, :wyposazenie_json, :ground_plan, :description,
        :contact, :owner
    )
    """
    try:
        cursor.execute(insert_query, data)
        conn.commit()
        log.debug(f'OK {id4}')
        conn.close()
        return 1
    except sqlite3.Error as ex:
        conn.rollback()
        log.warning(f'FAILED {data["url_id"]}, {ex}')
        conn.close()
        return 0


def insert_address_derrived(
    id4: str,
    city: str,
    postal_code: str,
    street: str,
    maps_url: str
) -> None:
    """
    Insert an address into the normalized_addresses table.
    """
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO normalized_addresses (url_id, city, postal_code, street, maps_url)
            VALUES (?, ?, ?, ?, ?)
        ''', (id4, city, postal_code, street, maps_url))
        conn.commit()
    except sqlite3.IntegrityError as ex:
        log.error(f'ALREADY EXISTS {id4}')
    finally:
        conn.close()