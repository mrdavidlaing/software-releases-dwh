CREATE USER dagster WITH PASSWORD 'dagster';

CREATE TABLE IF NOT EXISTS releases (
    product_id TEXT,
    version TEXT,
    name TEXT,
    release_date TIMESTAMP,
    link TEXT
);

--TODO: Introduce roles/groups
ALTER TABLE releases OWNER TO dagster;