import sqlite3

import psycopg
from psycopg.rows import dict_row

from src.database import queries
from src.utils.log_util import get_logger
import config


log = get_logger(__name__, 30, True, True)


PS = '%s' if config.OTODOM_DATABASE_TYPE == 'postgres' else '?'


def connect(db_type: str = config.OTODOM_DATABASE_TYPE) -> sqlite3.Connection | psycopg.Connection:
    """
    Connect to the database based on the provided type.
    """
    if db_type.lower() == 'postgres':
        return _connect_postgres()
    elif db_type.lower() == 'sqlite':
        return _connect_sqlite()
    else:
        raise ValueError(f'Unsupported database type: {db_type}')

def _connect_postgres() -> psycopg.Connection:
    """
    Connect to the PostgreSQL database.
    """

    conn = psycopg.connect(
        host=config.OTODOM_SERVER_NAME,
        port=config.OTODOM_SERVER_PORT,
        dbname=config.OTODOM_DATABASE_NAME,
        user=config.OTODOM_USERNAME,
        password=config.OTODOM_PASSWORD,
        row_factory=dict_row,
    )

    cur = conn.cursor()
    cur.execute(f'SET search_path TO {config.OTODOM_SCHEMA_NAME}')
    return conn


def _connect_sqlite() -> sqlite3.Connection:
    """
    Connect to the SQLite database.
    """
    conn = sqlite3.connect(config.OTODOM_DATABASE_NAME)
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
        cursor.execute(queries.Audit_Logs.DDL)
        cursor.execute(queries.Offers.DDL)
        cursor.execute(queries.Favorites.DDL)
        cursor.execute(queries.Normalized_Addresses.DDL)
        cursor.execute(queries.Run_Logs.DDL)
        cursor.execute(queries.Images.DDL)
        cursor.execute(queries.Date_Dim.DDL)
        cursor.execute(queries.Date_Dim.POPULATE)
        cursor.execute(queries.Views.offers_with_history.DDL)
        conn.commit()
        conn.close()
    except sqlite3.Error as exc:
        conn.rollback()
        conn.close()
        raise exc from exc


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


def upsert_offer(id4: str, entity: str, data: dict[str, str|int|None]) -> None:
    conn = connect()
    cursor = conn.cursor()
    data['url_id'] = id4
    data['entity'] = entity

    if get('offers', ['id'], [('url_id', id4)]):
        log.debug(f'SET HISTORICAL {id4}')
        q = f'UPDATE offers SET status = 2 WHERE url_id = {PS}'
        cursor.execute(q, (id4,))
        conn.commit()

    sqlite_placeholders = """
        :url_id, :status, :entity, :city, :postal_code, :street, :price, :area, :price_per_m2, :floors, :floor, :rooms,
        :build_year, :building_type, :building_material, :rent, :windows, :land_area, :construction_status, :market, :posted_by,
        :coordinates_lat_lon, :informacje_dodatkowe_json, :media_json, :ogrodzenie_json, :dojazd_json,
        :ogrzewanie_json, :okolica_json, :zabezpieczenia_json, :wyposazenie_json, :ground_plan, :images, :description,
        :contact, :owner
        """

    psycopg_placeholders = r"""
        %(url_id)s, %(status)s, %(entity)s, %(city)s, %(postal_code)s, %(street)s, %(price)s, %(area)s, %(price_per_m2)s, %(floors)s, %(floor)s, %(rooms)s,
        %(build_year)s, %(building_type)s, %(building_material)s, %(rent)s, %(windows)s, %(land_area)s, %(construction_status)s, %(market)s, %(posted_by)s,
        %(coordinates_lat_lon)s, %(informacje_dodatkowe_json)s, %(media_json)s, %(ogrodzenie_json)s, %(dojazd_json)s,
        %(ogrzewanie_json)s, %(okolica_json)s, %(zabezpieczenia_json)s, %(wyposazenie_json)s, %(ground_plan)s, %(images)s, %(description)s,
        %(contact)s, %(owner)s
        """

    insert_query = f"""
    INSERT INTO offers (
        url_id, status, entity, city, postal_code, street, price, area, price_per_m2, floors, floor, rooms,
        build_year, building_type, building_material, rent, windows, land_area, construction_status, market, posted_by,
        coordinates_lat_lon, informacje_dodatkowe_json, media_json, ogrodzenie_json, dojazd_json, ogrzewanie_json,
        okolica_json, zabezpieczenia_json, wyposazenie_json, ground_plan, images, description, contact, owner
    ) VALUES (
        {psycopg_placeholders if config.OTODOM_DATABASE_TYPE == 'postgres' else sqlite_placeholders}
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
