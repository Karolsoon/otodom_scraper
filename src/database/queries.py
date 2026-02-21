from config import OTODOM_DATABASE_TYPE


PS = '?' if OTODOM_DATABASE_TYPE.lower() == 'sqlite' else r'%s' #placeholder symbol for parametrized queries, different for sqlite and postgres
IDENTITY_CLAUSE = 'AUTOINCREMENT' if OTODOM_DATABASE_TYPE.lower() == 'sqlite' else 'GENERATED ALWAYS AS IDENTITY'
TIMESTAMP_TYPE = 'DATETIME' if OTODOM_DATABASE_TYPE.lower() == 'sqlite' else 'TIMESTAMP'
DATE_INCREMENT_BY_DAY = "date + INTERVAL '1 day'" if OTODOM_DATABASE_TYPE.lower() == 'postgres' else "date(date, '1 day')"
PAST_DAY_DECREMENT = "datetime('now', '-1 days')" if OTODOM_DATABASE_TYPE.lower() == 'sqlite' else "CURRENT_TIMESTAMP - INTERVAL '1 day'"
CAST_STR_TO_DATE = "CAST('2025-04-01 00:00:00' AS TIMESTAMP)" if OTODOM_DATABASE_TYPE.lower() == 'postgres' else "date('2025-04-01 00:00:00')"
CAST_DATE_PARTS = (
    """            CAST(strftime('%Y', date) AS INTEGER) AS year,
            CAST(strftime('%m', date) AS INTEGER) AS month,
            CAST(strftime('%d', date) AS INTEGER) AS day,
            CASE strftime('%w', date)"""
    if OTODOM_DATABASE_TYPE.lower() == 'sqlite' else
    """            EXTRACT(YEAR FROM date) AS year,
            EXTRACT(MONTH FROM date) AS month,
            EXTRACT(DAY FROM date) AS day,
            CASE EXTRACT(DOW FROM date)"""
)
CAST_CALENDAR_WEEK = "CAST(strftime('%W', date) AS INTEGER)" if OTODOM_DATABASE_TYPE.lower() == 'sqlite' else "EXTRACT(WEEK FROM date)"
CREATE_VIEW_CLAUSE = "CREATE VIEW IF NOT EXISTS" if OTODOM_DATABASE_TYPE.lower() == 'sqlite' else "CREATE OR REPLACE VIEW"
COORDINATES_SPLIT_EXPRESSION = "split_part(v.coordinates_lat_lon, ',', 1)::FLOAT" if OTODOM_DATABASE_TYPE.lower() == 'postgres' else "CAST(SUBSTR(v.coordinates_lat_lon, 1, INSTR(v.coordinates_lat_lon, ',') - 1) AS FLOAT)"


class Run_Logs:
    TABLE_NAME = 'run_logs'
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY {IDENTITY_CLAUSE},
            entity TEXT NOT NULL,
            started_at {TIMESTAMP_TYPE} NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at {TIMESTAMP_TYPE} NULL,
            is_success BOOLEAN NOT NULL DEFAULT false
        );
    """
    create_log = f"""
        INSERT INTO {TABLE_NAME} (entity, started_at) VALUES
            ({PS}, {PS})
        RETURNING id;
    """
    update_finished_and_status = f"""
        UPDATE {TABLE_NAME}
        SET finished_at = {PS},
            is_success = {PS}
        WHERE id = {PS}
    """


class Urls:
    TABLE_NAME = 'urls'
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY {IDENTITY_CLAUSE},
            url_id TEXT NOT NULL,
            url TEXT NOT NULL,
            status INT NOT NULL,
            created_run_id INTEGER NOT NULL,
            updated_run_id INTEGER NULL,
            expired_run_id INTEGER NULL
        );
    """
    get_status = f"""
        SELECT status
        FROM {TABLE_NAME}
        WHERE url_id = {PS}
        ORDER BY id DESC
        LIMIT 1;
    """
    create_if_not_exists = f"""
        WITH check_existence AS (
            SELECT 1 AS exists_flag
            FROM {TABLE_NAME}
            WHERE url_id = {PS}
            AND status = 2
            LIMIT 1
        )
        INSERT INTO {TABLE_NAME} (url_id, url, status, created_run_id, updated_run_id)
        SELECT 
            {PS} as url_id,
            {PS} as url,
            {PS} as status,
            {PS} as created_run_id,
            {PS} as updated_run_id
        WHERE NOT EXISTS (
            SELECT 1 FROM {TABLE_NAME} WHERE url_id = {PS} AND status = 1)
        RETURNING
            COALESCE((SELECT exists_flag FROM check_existence), 0) AS existence_flag;
    """
    update_status = f"""
        UPDATE {TABLE_NAME}
        SET status = {PS}, updated_run_id = {PS}, expired_run_id = {PS}
        WHERE url_id = {PS} and status = 1
    """
    get_active_urls_by_entity = f"""
        SELECT url
        FROM {TABLE_NAME} u
        LEFT OUTER JOIN {Run_Logs.TABLE_NAME} r ON r.id = u.created_run_id
        WHERE u.status = 1
          AND r.entity = {PS}
    """


class Audit_Logs:
    TABLE_NAME = 'audit_logs'
    idx = f'CREATE INDEX IF NOT EXISTS audit_logs_url_id_idx ON {TABLE_NAME} (url_id);' if OTODOM_DATABASE_TYPE.lower() == 'postgres' else ''
    idx2= f'CREATE INDEX IF NOT EXISTS audit_logs_run_id_idx ON {TABLE_NAME} (run_id);' if OTODOM_DATABASE_TYPE.lower() == 'postgres' else ''
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY {IDENTITY_CLAUSE},
            run_id INTEGER NOT NULL,
            url_id TEXT NOT NULL,
            html_file_path TEXT NOT NULL,
            status_code TEXT NULL,
            created_at {TIMESTAMP_TYPE} NULL DEFAULT CURRENT_TIMESTAMP,
            visited_at {TIMESTAMP_TYPE} NULL,
            parsed_at {TIMESTAMP_TYPE} NULL,
            error_step TEXT NULL,
            error_message TEXT NULL
        );
        {idx}
        {idx2}
    """
    create_log = f"""
        INSERT INTO {TABLE_NAME} (run_id, url_id, html_file_path, created_at) VALUES
            ({PS}, {PS}, {PS}, {PS});
    """
    update_visited = f"""
        UPDATE {TABLE_NAME}
        SET visited_at = {PS},
            status_code = {PS},
            error_step = {PS},
            error_message = {PS}
        WHERE id = {PS}
    """
    update_parsed = f"""
        UPDATE {TABLE_NAME}
        SET parsed_at = {PS},
            error_step = {PS},
            error_message = {PS}
        WHERE id = {PS}
    """
    get_for_download = f"""
        SELECT l.id, l.url_id, u.url
        FROM {TABLE_NAME} l
        LEFT OUTER JOIN {Urls.TABLE_NAME} u ON u.url_id = l.url_id
        WHERE l.visited_at IS NULL
    """
    get_for_parsing = f"""
        SELECT l.id, l.url_id, l.visited_at, l.html_file_path as filepath, u.url
        FROM {TABLE_NAME} l
        LEFT OUTER JOIN {Urls.TABLE_NAME} u ON u.url_id = l.url_id
        WHERE l.visited_at IS NOT NULL
          AND l.parsed_at IS NULL
    """


class Favorites:
    TABLE_NAME = 'favorites'
    DDL = f"""
       CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY {IDENTITY_CLAUSE},
            url_id TEXT NOT NULL,
            UNIQUE(url_id)
        );
    """


class Offers:
    TABLE_NAME = 'offers'
    idx = f'CREATE INDEX IF NOT EXISTS offers_url_id_idx ON {TABLE_NAME} (url_id);' if OTODOM_DATABASE_TYPE.lower() == 'postgres' else ''
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY {IDENTITY_CLAUSE},
            url_id TEXT NOT NULL,
            "status" INTEGER NOT NULL,
            entity TEXT NOT NULL,
            city TEXT NOT NULL,
            postal_code TEXT NULL,
            street TEXT NULL,
            price INTEGER NULL,
            area NUMERIC NULL,
            price_per_m2 NUMERIC NULL,
            floors INTEGER NULL,
            floor INTEGER NULL,
            rooms INTEGER NULL,
            "build_year" INTEGER NULL,
            building_type TEXT NULL,
            building_material TEXT NULL,
            rent NUMERIC NULL,
            "windows" TEXT NULL,
            land_area NUMERIC NULL,
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
            images TEXT NULL,
            description TEXT NULL,
            contact TEXT NOT NULL,
            "owner" TEXT NULL,
            created_at {TIMESTAMP_TYPE} NULL DEFAULT CURRENT_TIMESTAMP
        );
        {idx}

    """


class Images:
    TABLE_NAME = 'images'
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY {IDENTITY_CLAUSE},
            url_id TEXT NOT NULL,
            image_id TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            location TEXT NOT NULL,
            "type" TEXT NOT NULL, -- floor_plan OR real_estate
            created_at {TIMESTAMP_TYPE} NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(url_id, image_id)
        );
    """
    get_images_to_download_by_url_id = f"""
        SELECT DISTINCT o.url_id, o.images
        FROM {Offers.TABLE_NAME} o
        LEFT JOIN {TABLE_NAME} i ON i.url_id = o.url_id
        WHERE i.image_id IS NULL
          AND o.images IS NOT NULL
          AND o.url_id = {PS}
    """
    get_all_images_to_download = f"""
        SELECT DISTINCT o.url_id, o.images
        FROM {Offers.TABLE_NAME} o
        LEFT JOIN {TABLE_NAME} i ON i.url_id = o.url_id
        WHERE o.images IS NOT NULL
    """
    get_image_id = f"""
        SELECT url_id, image_id
        FROM {TABLE_NAME} i
        WHERE url_id = {PS}
          AND image_id = {PS}
    """
    create_image_entry = f"""
        INSERT INTO images (url_id, image_id, status_code, location, type) VALUES
        ({PS}, {PS}, {PS}, {PS}, {PS});
    """
    get_all_downloaded_image_paths = f"""
        SELECT location
        FROM {TABLE_NAME}
    """


class Normalized_Addresses:
    TABLE_NAME = 'normalized_addresses'
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY {IDENTITY_CLAUSE},
            url_id TEXT NOT NULL,
            city TEXT NOT NULL,
            postal_code TEXT NOT NULL,
            street TEXT NOT NULL,
            coordinates_lat_lon TEXT NOT NULL,
            maps_url TEXT NOT NULL,
            created_at {TIMESTAMP_TYPE} NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (url_id, street, coordinates_lat_lon)
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
    insert_address = f"""
        INSERT INTO {TABLE_NAME} (url_id, city, postal_code, street, maps_url, coordinates_lat_lon)
        VALUES ({PS}, {PS}, {PS}, {PS}, {PS}, {PS});
    """


class Notifications:
    TABLE_NAME = 'notifications'
    DDL = f'''
        create table if not exists {TABLE_NAME} (
            id integer generated always as identity,
            url_id varchar(10) not null,
            price integer not null,
            ts timestamp null default current_timestamp
        );'''
    
    insert_notification = f"""
        INSERT INTO {TABLE_NAME} (url_id, price)
        VALUES ({PS}, {PS});
    """


class Date_Dim:
    TABLE_NAME = 'date_dim'
    DDL = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date {TIMESTAMP_TYPE}    PRIMARY KEY,          -- The date in YYYY-MM-DD format
            year INTEGER NOT NULL,          -- The year (e.g., 2025)
            month INTEGER NOT NULL,         -- The month (1-12)
            day INTEGER NOT NULL,           -- The day of the month (1-31)
            day_of_week TEXT NOT NULL,      -- The name of the day (e.g., Monday)
            calendar_week INTEGER NOT NULL  -- The calendar week number (1-53)
        );
    """
    POPULATE = f"""
        -- Populate the date_dimension table
        WITH RECURSIVE date_series AS (
            SELECT {CAST_STR_TO_DATE} AS "date"
            UNION ALL
            SELECT {DATE_INCREMENT_BY_DAY} AS "date"
            FROM date_series
            WHERE date < date('2030-04-01')
        )
        INSERT INTO {TABLE_NAME} (date, year, month, day, day_of_week, calendar_week)
        SELECT 
            date,
            {CAST_DATE_PARTS}
                WHEN '0' THEN 'Sunday'
                WHEN '1' THEN 'Monday'
                WHEN '2' THEN 'Tuesday'
                WHEN '3' THEN 'Wednesday'
                WHEN '4' THEN 'Thursday'
                WHEN '5' THEN 'Friday'
                WHEN '6' THEN 'Saturday'
            END AS day_of_week,
            {CAST_CALENDAR_WEEK} AS calendar_week
        FROM date_series
        WHERE NOT EXISTS (
            SELECT 1 FROM {TABLE_NAME} WHERE date = date_series.date
        );
    """


class Statistics:
    offers_count_per_day = f"""
        -- QUERY FOR RETURNING THE TOTAL OFFER COUNT PER DAY
        -- Oferty aktualne od daty utworzenia
        WITH current_offers AS (
            SELECT DISTINCT
                u.id,
                datetime(strftime('%Y-%m-%dT00:00:00', rl.started_at)) AS created_at
            FROM {Urls.TABLE_NAME} u
            INNER JOIN {Run_Logs.TABLE_NAME} rl ON rl.id = u.created_run_id
            GROUP BY u.id
            HAVING u.status = 1
        ),

        expired_offers AS (
            -- Oferty nieaktualne z datą ostatniej aktualizacji
            SELECT DISTINCT
                u.id,
                MAX(datetime(strftime('%Y-%m-%dT00:00:00', rl.started_at))) AS updated_at
            FROM {Urls.TABLE_NAME} u
            INNER JOIN {Run_Logs.TABLE_NAME} rl ON rl.id = u.updated_run_id
            GROUP BY u.id
            HAVING u.status = 2
        )

        SELECT
            dm.date AS "date",
            CASE 
                WHEN dm.date > date('now') THEN NULL
                ELSE COUNT(DISTINCT co.id) + COUNT(DISTINCT eo.id)
            END AS distinct_count
        FROM {Date_Dim.TABLE_NAME} dm
        LEFT OUTER JOIN current_offers co ON co.created_at <= dm.date
        LEFT OUTER JOIN expired_offers eo ON eo.updated_at = dm.date
        WHERE dm.date > '2025-05-01'
        GROUP BY dm.date
        HAVING dm.date <= date('now');
    """

class Views:
    class offers_with_history:
        TABLE_NAME = 'offers_with_history'
        DDL = f"""
            {CREATE_VIEW_CLAUSE} v_offers_change_history_all AS 
                SELECT
                    RANK() OVER(PARTITION BY o.url_id ORDER BY o.created_at DESC) AS most_recent_order,
                    o.id,
                    rl.id AS created_run_id,
                    rl.started_at AS created_at,
                    rl.started_at AS updated_at,
                    o.url_id,
                    o.status,
                    rl.entity,
                    o.city,
                    COALESCE(na.postal_code, o.postal_code) AS postal_code,
                    COALESCE(o.street, na.street) AS street,
                    o.price,
                    o.area,
                    o.price_per_m2,
                    o.floors,
                    o.floor,
                    o.rooms,
                    o.build_year,
                    o.building_type,
                    o.building_material,
                    o.rent,
                    o.construction_status,
                    o.market,
                    o.posted_by,
                    o.coordinates_lat_lon,
                    o.informacje_dodatkowe_json,
                    o.media_json,
                    o.ogrodzenie_json,
                    o.dojazd_json,
                    o.ogrzewanie_json,
                    o.okolica_json,
                    o.zabezpieczenia_json,
                    o.wyposazenie_json,
                    o.ground_plan,
                    na.maps_url,
                    u.url,
                    o.description
                FROM offers o
                LEFT OUTER JOIN normalized_addresses na ON na.url_id = o.url_id
                LEFT OUTER JOIN "urls" u ON u.url_id = o.url_id
                LEFT OUTER JOIN run_logs rl ON rl.id = u.created_run_id
                LEFT OUTER JOIN run_logs rl2 ON rl.id = u.updated_run_id
                GROUP BY o.id, rl.id, na.postal_code, na.street, na.maps_url, u.url, o.url_id, o.price, o.area, o.price_per_m2, o.floors, o.floor, o.rooms, o.build_year, o.construction_status, o.market, o.coordinates_lat_lon, o.status
                ORDER BY o.url_id DESC, most_recent_order ASC;
        """
        get_all = f"""
            SELECT * FROM offers_change_history_all;
        """
        get_all_latest = f"""
            SELECT * FROM offers_change_history_all
            WHERE most_recent_order = 1;
        """
        get_all_active_latest = f"""
            SELECT * FROM offers_change_history_all
            WHERE most_recent_order = 1
              AND status = 1;
        """
        get_all_expired_latest = f"""
            SELECT * FROM offers_change_history_all
            WHERE most_recent_order = 1
              AND status = 2;
        """
        get_by_url_id = f"""
            SELECT * FROM offers_change_history_all
            WHERE url_id = {PS};
        """
        get_latest_by_url_id = f"""
            SELECT * FROM offers_change_history_all
            WHERE url_id = {PS}
              AND most_recent_order = 1;
        """

    class offers_with_previous_price:
        TABLE_NAME = 'v_offers_with_previous_price'
        DDL = f'''
            {CREATE_VIEW_CLAUSE} v_offers_with_previous_price AS        
            select
                RANK() OVER(PARTITION BY o.url_id ORDER BY o.created_at DESC) AS most_recent_order,
                o.id,
                o.url_id,
                o.status,
                o.entity,
                o.city,
                COALESCE(na.postal_code, o.postal_code) AS postal_code,
                COALESCE(o.street, na.street) AS street,
                o.price,
                lag(o.price, -1) OVER(partition by o.url_id) - o.price as price_diff,
                lag(o.price, -1) OVER(partition by o.url_id) as price_previous,
                o.area,
                o.price_per_m2,
                o.floors,
                o.floor,
                o.rooms,
                o.build_year,
                o.building_type,
                o.building_material,
                o.rent,
                o.windows,
                o.land_area,
                o.construction_status,
                o.market,
                o.posted_by,
                o.coordinates_lat_lon,
                o.informacje_dodatkowe_json,
                o.media_json,
                o.ogrodzenie_json,
                o.dojazd_json,
                o.ogrzewanie_json,
                o.okolica_json,
                o.zabezpieczenia_json,
                o.wyposazenie_json,
                o.ground_plan,
                o.images,
                o.description,
                o.contact,
                o.owner,
                o.created_at,
                na.maps_url
            FROM offers o
            left outer join normalized_addresses na on na.url_id = o.url_id
            order by o.url_id desc, o.created_at desc;
'''
    get_all_images_to_download = f"""
        SELECT DISTINCT o.url_id, o.images
        FROM {offers_with_previous_price.TABLE_NAME} o
        LEFT JOIN images i ON i.url_id = o.url_id
        WHERE o.images IS NOT null
          and o.most_recent_order = 1
          and o.status = 1
    """

class Watchdog:
    get_new_interesting_offers_last_1_day = f"""
        SELECT DISTINCT v.most_recent_order, u.url_id, v.url, v.status, v.entity, v.city, v.street, v.price, v.price_per_m2, v.area, v.rooms, v.floor, v.maps_url, v.created_at
        FROM v_offers_change_history_all v
        LEFT OUTER JOIN urls u ON u.url_id = v.url_id
        LEFT OUTER JOIN run_logs rl ON rl.id = v.created_run_id  
        WHERE rl.started_at > {PAST_DAY_DECREMENT}
        AND v.construction_status IN ('ready_to_use', 'to_completion')
        AND (LOWER(v.building_type) <> 'ribbon' OR v.building_type IS NULL)
        AND v.most_recent_order = 1
        AND {COORDINATES_SPLIT_EXPRESSION} < 51.6712
        AND v.city NOT IN ('Białołęka', 'Bucze', 'Trzebcz', 'Wilków', 'Serby', 'Grodziec Mały', 'Pęcław', 'Kaczyce', 'Kotla')
        AND (LOWER(v.description) NOT LIKE '%do remontu%' AND LOWER(v.description) NOT LIKE '%całkowitego remontu%')
        AND (
            (v.city = 'Głogów' AND v.entity IN ('flats', 'houses_glogow'))
            OR (v.city <> 'Głogów' AND v.entity IN ('houses_glogow', 'houses_radwanice'))
        )
        AND (
                (v.street LIKE '%łowiańska%')
            OR (v.rooms > 3 AND v.area > 75 AND v.floor < 3 AND v.entity = 'flats' AND v.market = 'secondary' AND v.price <= 650000)
            OR (v.rooms > 3 AND v.area > 75 AND v.entity = 'flats' AND (v.market = 'primary' OR v.construction_status = 'to_completion') AND v.price <= 600000)
            OR (v.rooms > 4 AND price < 550000)
            OR (v.rooms > 2 AND v.area > 100 AND v.price < 400000)
            OR (v.rooms > 3 AND v.construction_status = 'ready_to_use' AND v.price < 900000 AND v.wyposazenie_json <> '[]')
            OR (v.rooms > 4 AND v.entity IN ('houses_glogow', 'houses_radwanice') AND v.price < 550000)
            OR (v.rooms > 4 AND v.entity IN ('houses_glogow', 'houses_radwanice') AND v.price < 900000 AND v.construction_status = 'ready_to_use')
            OR (v.rooms > 3 AND v.entity = 'houses_radwanice' AND v.price < 350000)
            OR (v.rooms = -1)
        )
        AND v.status = 1
    """
    get_all_interesting_offers_incl_expired = f"""
        SELECT DISTINCT v.most_recent_order, u.url_id, v.url, v.status, v.entity, v.city, v.street, v.price, v.price_per_m2, v.area, v.rooms, v.floor, v.maps_url, v.created_at
        FROM v_offers_change_history_all v
        LEFT OUTER JOIN urls u ON u.url_id = v.url_id
        WHERE v.construction_status IN ('ready_to_use', 'to_completion')
        AND (LOWER(v.building_type) <> 'ribbon' OR v.building_type IS NULL)
        AND v.most_recent_order = 1
        AND CAST(SUBSTR(v.coordinates_lat_lon, 1, INSTR(v.coordinates_lat_lon, ',') - 1) AS FLOAT) < 51.6712
        AND v.city NOT IN ('Białołęka', 'Bucze', 'Trzebcz', 'Wilków', 'Serby', 'Grodziec Mały', 'Pęcław', 'Kaczyce', 'Kotla')
        AND (LOWER(v.description) NOT LIKE '%do remontu%' AND LOWER(v.description) NOT LIKE '%całkowitego remontu%')
        AND (
            (v.city = 'Głogów' AND v.entity IN ('flats', 'houses_glogow'))
            OR (v.city <> 'Głogów' AND v.entity IN ('houses_glogow', 'houses_radwanice'))
        )
        AND (
                (v.street LIKE '%łowiańska%')
            OR (v.rooms > 3 AND v.area > 75 AND v.floor < 3 AND v.entity = 'flats' AND v.market = 'secondary' AND v.price <= 650000)
            OR (v.rooms > 3 AND v.area > 75 AND v.entity = 'flats' AND (v.market = 'primary' OR v.construction_status = 'to_completion') AND v.price <= 600000)
            OR (v.rooms > 4 AND price < 550000)
            OR (v.rooms > 2 AND v.area > 100 AND v.price < 400000)
            OR (v.rooms > 3 AND v.construction_status = 'ready_to_use' AND v.price < 900000 AND v.wyposazenie_json <> '[]')
            OR (v.rooms > 4 AND v.entity IN ('houses_glogow', 'houses_radwanice') AND v.price < 550000)
            OR (v.rooms > 4 AND v.entity IN ('houses_glogow', 'houses_radwanice') AND v.price < 900000 AND v.construction_status = 'ready_to_use')
            OR (v.rooms > 3 AND v.entity = 'houses_radwanice' AND v.price < 350000)
            OR (v.rooms = -1)
        )
        ORDER BY v.created_at DESC, v.most_recent_order ASC, v.price_per_m2 ASC
    """

    get_most_recent_interesting_offers = f'''
        select v.*, u.url
        from {Views.offers_with_previous_price.TABLE_NAME} v
        left outer join urls u on u.url_id = v.url_id and u.status = 1
        left outer join notifications n on n.url_id = v.url_id and n.price = v.price
        where v.most_recent_order IN (1, 2, 3, 4)
            and v.created_at > CURRENT_TIMESTAMP - INTERVAL '1 day'
            and v.status = 1
            AND (v.construction_status IN ('ready_to_use', 'to_completion') OR v.construction_status IS NULL)
            --AND (LOWER(v.building_type) <> 'ribbon' OR v.building_type IS NULL)
            AND split_part(v.coordinates_lat_lon, ',', 1)::FLOAT < 51.6712
            AND LOWER(v.city) NOT IN ('białołęka', 'bucze', 'trzebcz', 'wilków', 'serby', 'grodziec mały', 'pęcław', 'kaczyce', 'kotla', 'krzydłowice')
            AND (LOWER(v.description) NOT LIKE '%do remontu%' AND LOWER(v.description) NOT LIKE '%całkowitego remontu%')
            AND (
                (LOWER(v.city) = 'głogów' AND v.entity IN ('flats', 'houses_glogow'))
                OR (LOWER(v.city) <> 'głogów' AND v.entity IN ('houses_glogow', 'houses_radwanice'))
            )
            AND (
                    (v.street LIKE '%łowiańska%' or v.street like '%waldzka%')
                OR (v.rooms > 3 AND v.area > 75 AND v.floor < 3 AND v.entity = 'flats' AND v.market = 'secondary' AND v.price <= 750000)
                OR (v.rooms > 3 AND v.area > 75 AND v.entity = 'flats' AND (v.market = 'primary' OR v.construction_status = 'to_completion') AND v.price <= 550000)
                OR (v.rooms > 4 AND v.price < 550000)
                OR (v.rooms > 2 AND v.area > 100 AND v.price < 400000)
                OR (v.rooms > 3 AND v.construction_status = 'ready_to_use' AND v.price < 900000 AND v.wyposazenie_json <> '[]')
                OR (v.rooms > 4 AND v.entity IN ('houses_glogow', 'houses_radwanice') AND v.price < 550000)
                OR (v.rooms > 4 AND v.entity IN ('houses_glogow', 'houses_radwanice') AND v.price < 900000 AND v.construction_status = 'ready_to_use')
                OR (v.rooms > 3 AND v.entity = 'houses_radwanice' AND v.price < 450000)
                OR (v.rooms = -1)
            )
			AND n.url_id is null;'''