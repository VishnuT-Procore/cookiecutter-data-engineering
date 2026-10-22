-- One-time Snowflake setup for {{ cookiecutter.project_slug }}. Run as SECURITYADMIN / SYSADMIN.
-- Replace the <...> placeholders (public keys come from `openssl rsa -in key.p8 -pubout`).

use role sysadmin;
create database if not exists {{ cookiecutter.project_slug | upper }};
create schema if not exists {{ cookiecutter.project_slug | upper }}.{{ cookiecutter.raw_schema | upper }};
create warehouse if not exists TRANSFORMING warehouse_size = xsmall auto_suspend = 60 auto_resume = true
    initially_suspended = true;

use role securityadmin;

-- Role used by the Kafka connector: writes raw tables only.
create role if not exists KAFKA_CONNECTOR;
grant usage on database {{ cookiecutter.project_slug | upper }} to role KAFKA_CONNECTOR;
grant usage, create table on schema {{ cookiecutter.project_slug | upper }}.{{ cookiecutter.raw_schema | upper }} to role KAFKA_CONNECTOR;

create user if not exists KAFKA_CONNECTOR_USER
    type = service
    default_role = KAFKA_CONNECTOR
    rsa_public_key = '<connector public key>';
grant role KAFKA_CONNECTOR to user KAFKA_CONNECTOR_USER;

-- Role used by dbt: reads raw, owns bronze/silver/gold.
create role if not exists TRANSFORMER;
grant usage on warehouse TRANSFORMING to role TRANSFORMER;
grant usage, create schema on database {{ cookiecutter.project_slug | upper }} to role TRANSFORMER;
grant usage on schema {{ cookiecutter.project_slug | upper }}.{{ cookiecutter.raw_schema | upper }} to role TRANSFORMER;
grant select on all tables in schema {{ cookiecutter.project_slug | upper }}.{{ cookiecutter.raw_schema | upper }} to role TRANSFORMER;
grant select on future tables in schema {{ cookiecutter.project_slug | upper }}.{{ cookiecutter.raw_schema | upper }} to role TRANSFORMER;

create user if not exists DBT_USER
    type = service
    default_role = TRANSFORMER
    default_warehouse = TRANSFORMING
    rsa_public_key = '<dbt public key>';
grant role TRANSFORMER to user DBT_USER;

grant role KAFKA_CONNECTOR to role sysadmin;
grant role TRANSFORMER to role sysadmin;
