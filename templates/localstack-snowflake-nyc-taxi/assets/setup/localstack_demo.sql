/* @bruin
name: setup.localstack_demo
type: sf.sql
description: Creates the warehouse, database and schemas on the emulator. It logs in through snowflake-localstack-setup, which names no database, because a login that names LOCALSTACK_DEMO fails until it exists. Creating the schemas up front, plus ingestr's _BRUIN_STAGING, keeps parallel assets from racing to create them. Safe to rerun.
connection: snowflake-localstack-setup
@bruin */

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
CREATE DATABASE IF NOT EXISTS LOCALSTACK_DEMO;
CREATE SCHEMA IF NOT EXISTS LOCALSTACK_DEMO.BRONZE;
CREATE SCHEMA IF NOT EXISTS LOCALSTACK_DEMO.SILVER;
CREATE SCHEMA IF NOT EXISTS LOCALSTACK_DEMO.GOLD;
CREATE SCHEMA IF NOT EXISTS LOCALSTACK_DEMO._BRUIN_STAGING;
