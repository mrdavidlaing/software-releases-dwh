CREATE TABLE IF NOT EXISTS software_releases_dwh.dim_releases (
    product_id TEXT,
    version TEXT,
    name TEXT,
    release_date TIMESTAMP,
    link TEXT
);

--TODO: Introduce roles/groups
CREATE USER dagster WITH PASSWORD 'dagster';
GRANT USAGE ON SCHEMA software_releases_dwh TO dagster;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA software_releases_dwh TO dagster;