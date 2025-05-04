class Urls:
    TABLE_NAME = 'urls'
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_id TEST NOT NULL,
            url TEXT NOT NULL,
            entity TEXT NOT NULL,
            status INT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            expired_at DATETIME DEFAULT NULL
        );
    """


class Audit_Logs:
    TABLE_NAME = 'audit_logs'
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            url_id TEXT NOT NULL,
            html_file_path TEXT NOT NULL,
            status_code TEXT NULL,
            created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP,
            visited_at DATETIME NULL,
            parsed_at DATETIME NULL,
            error_step TEXT NULL,
            error_message TEXT NULL
        );
    """
    create_log = f"""
        INSERT INTO {TABLE_NAME} (run_id, url_id, html_file_path, created_at) VALUES
            (?, ?, ?, ?);
    """
    update_visited = f"""
        UPDATE {TABLE_NAME}
        SET visited_at = ?,
            status_code = ?,
            error_step = ?,
            error_message = ?
        WHERE id = ?
    """
    update_parsed = f"""
        UPDATE {TABLE_NAME}
        SET parsed_at = ?,
            error_step = ?,
            error_message = ?
        WHERE id = ?
    """
    get_for_download = f"""
        SELECT l.id, l.url_id, u.url
        FROM {TABLE_NAME} l
        LEFT OUTER JOIN {Urls.TABLE_NAME} u ON u.url_id = l.url_id
        WHERE l.visited_at IS NULL
    """
    get_for_parsing = f"""
        SELECT l.id, l.url_id, l.visited_at, l.html_file_path as filepath
        FROM {TABLE_NAME} l
        WHERE l.visited IS NOT NULL
          AND l.parsed_at IS NULL
    """


class Run_Logs:
    TABLE_NAME = 'run_logs'
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity TEXT NOT NULL,
            url_list_path TEXT NULL,
            started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at DATETIME NULL,
            is_success BOOLEAN NOT NULL DEFAULT false
        );
    """
    create_log = f"""
        INSERT INTO {TABLE_NAME} (entity, started_at) VALUES
            (?, ?)
        RETURNING id;
    """
    update_finished_and_status = f"""
        UPDATE {TABLE_NAME}
        SET finished_at = ?,
            is_success = ?
        WHERE id = ?
    """


class Offers:
    TABLE_NAME = 'offers'
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_id TEXT NOT NULL,
            "status" INTEGER NOT NULL,
            entity TEXT NOT NULL,
            city TEXT NOT NULL,
            postal_code TEXT NULL,
            street TEXT NULL,
            price INTEGER NOT NULL,
            area INTEGER NOT NULL,
            price_per_m2 INTEGER NOT NULL,
            floors INTEGER NULL,
            floor INTEGER NULL,
            rooms INTEGER NOT NULL,
            "build_year" INTEGER NULL,
            building_type TEXT NULL,
            building_material TEXT NULL,
            rent INTEGER NULL,
            "windows" TEXT NULL,
            land_area INTEGER NULL,
            construction_status TEXT NULL,
            market TEXT NULL,
            posted_by TEXT NULL,
            coordinates_lat_lon TEXT NULL,
            informacje_dodatkowe_json TEXT NULL,
            media_json TEXT NULL,
            ogrodzenie_json TEXT NULL,
            dojazd_json TEXT NULL,
            ogrzewanie_json TEXT NULL,
            okolica_json TEXT NULL,
            zabezpieczenia_json TEXT NULL,
            wyposazenie_json TEXT NULL,
            ground_plan TEXT NULL,
            description TEXT NULL,
            contact TEXT NOT NULL,
            "owner" TEXT NULL,
            created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP
        );
    """


class Normalized_Addresses:
    TABLE_NAME = 'normalized_addresses'
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_id TEXT NOT NULL,
            city TEXT NOT NULL,
            postal_code TEXT NOT NULL,
            street TEXT NOT NULL,
            coordinates_lat_lon TEXT NOT NULL,
            maps_url TEXT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (url_id, city, postal_code, street)
        );
    """
    get_coordinates_to_add = f"""
        SELECT DISTINCT
            o.url_id,
            o.coordinates_lat_lon
        FROM {Offers.TABLE_NAME} o
        LEFT OUTER JOIN {TABLE_NAME} n ON n.url_id = o.url_id
        WHERE NOT EXISTS (
            SELECT 1 FROM {TABLE_NAME} WHERE url_id = o.url_id AND coordinates_lat_lon = o.coordinates_lat_lon
        );
    """


class Favorites:
    TABLE_NAME = 'favorites'
    DDL = f"""
       CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_id TEXT NOT NULL,
            UNIQUE(url_id, status)
        );
    """
