-- One-time Snowflake setup for {{ cookiecutter.project_slug }}. Replace the <...> placeholders.
-- Snowflake reads the Iceberg landing tables through a catalog integration (the same Iceberg REST
-- catalog Flink writes to) and an external volume (the object storage holding the data files).
-- Reference: CREATE CATALOG INTEGRATION (Apache Iceberg REST), CREATE EXTERNAL VOLUME, CREATE ICEBERG TABLE.

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS {{ cookiecutter.project_slug | upper }};
CREATE WAREHOUSE IF NOT EXISTS TRANSFORMING WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Object storage of the landing tables (example for S3; see the docs for Azure/GCS).
CREATE EXTERNAL VOLUME IF NOT EXISTS {{ cookiecutter.project_slug | upper }}_LANDING_VOLUME
  STORAGE_LOCATIONS = ((
    NAME = 'landing'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = '<s3://bucket/warehouse/>'
    STORAGE_AWS_ROLE_ARN = '<arn:aws:iam::123456789012:role/snowflake-landing-read>'
  ))
  ALLOW_WRITES = FALSE;

-- The Iceberg REST catalog Flink writes to (Apache Polaris / Snowflake Open Catalog, AWS Glue, Unity Catalog, ...).
CREATE CATALOG INTEGRATION IF NOT EXISTS {{ cookiecutter.project_slug | upper }}_LANDING_CATALOG
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = '{{ cookiecutter.landing_namespace }}'
  REST_CONFIG = (
    CATALOG_URI = '<https://catalog.example.com/api/catalog>'
    CATALOG_NAME = '<warehouse>'
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_CLIENT_ID = '<client-id>'
    OAUTH_CLIENT_SECRET = '<client-secret>'
    OAUTH_ALLOWED_SCOPES = ('<scope>')
  )
  ENABLED = TRUE;

SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('{{ cookiecutter.project_slug | upper }}_LANDING_CATALOG');

-- Role used by dbt: reads the landing tables, owns bronze/silver/gold.
CREATE ROLE IF NOT EXISTS TRANSFORMER;
GRANT USAGE ON WAREHOUSE TRANSFORMING TO ROLE TRANSFORMER;
GRANT USAGE, CREATE SCHEMA ON DATABASE {{ cookiecutter.project_slug | upper }} TO ROLE TRANSFORMER;
GRANT USAGE ON EXTERNAL VOLUME {{ cookiecutter.project_slug | upper }}_LANDING_VOLUME TO ROLE TRANSFORMER;
GRANT USAGE ON INTEGRATION {{ cookiecutter.project_slug | upper }}_LANDING_CATALOG TO ROLE TRANSFORMER;

CREATE USER IF NOT EXISTS DBT_USER
    TYPE = SERVICE
    DEFAULT_ROLE = TRANSFORMER
    DEFAULT_WAREHOUSE = TRANSFORMING
    RSA_PUBLIC_KEY = '<dbt public key>';
GRANT ROLE TRANSFORMER TO USER DBT_USER;

-- Next: run snowflake/landing_tables.sql (generated from config/pipeline.yml).
