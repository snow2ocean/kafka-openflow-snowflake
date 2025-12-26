-- ============================================================================
-- Copyright 2025 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
-- Licensed under the Apache License, Version 2.0 (the "License");
-- You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
-- ============================================================================
--
-- Openflow Kafka Connector Quickstart - Snowflake Setup
--
-- This script sets up all required Snowflake objects for the Kafka log streaming demo
-- Run this BEFORE configuring the Openflow connector
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Step 1: Create Role and Database
-- ----------------------------------------------------------------------------

-- Create runtime role (reuse if coming from SPCS quickstart)
CREATE ROLE IF NOT EXISTS KAFKA_OPENFLOW_ROLE;

-- Create database for Kafka streaming data
CREATE DATABASE IF NOT EXISTS KAFKA_OPENFLOW_DB;

-- Create warehouse for data processing and queries
CREATE WAREHOUSE IF NOT EXISTS KAFKA_OPENFLOW_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Grant privileges to runtime role
GRANT OWNERSHIP ON DATABASE KAFKA_OPENFLOW_DB TO ROLE KAFKA_OPENFLOW_ROLE;
GRANT OWNERSHIP ON SCHEMA KAFKA_OPENFLOW_DB.PUBLIC TO ROLE KAFKA_OPENFLOW_ROLE;
GRANT USAGE ON WAREHOUSE KAFKA_OPENFLOW_WH TO ROLE KAFKA_OPENFLOW_ROLE;
GRANT CREATE OPENFLOW DATA PLANE INTEGRATION ON ACCOUNT TO ROLE KAFKA_OPENFLOW_ROLE;
GRANT CREATE OPENFLOW RUNTIME INTEGRATION ON ACCOUNT TO ROLE KAFKA_OPENFLOW_ROLE;
-- Create database for openflow
CREATE DATABASE IF NOT EXISTS OPENFLOW;

USE OPENFLOW;

CREATE SCHEMA IF NOT EXISTS OPENFLOW;

USE SCHEMA OPENFLOW;
CREATE IMAGE REPOSITORY IF NOT EXISTS OPENFLOW;

-- GRANT REQUIRED PREVILLEGES
GRANT USAGE ON DATABASE OPENFLOW TO ROLE PUBLIC,KAFKA_OPENFLOW_ROLE;
GRANT USAGE ON SCHEMA OPENFLOW TO ROLE PUBLIC,KAFKA_OPENFLOW_ROLE;
GRANT READ ON IMAGE REPOSITORY OPENFLOW.OPENFLOW.OPENFLOW TO ROLE PUBLIC,KAFKA_OPENFLOW_ROLE;
-- Grant runtime role to Openflow admin
GRANT ROLE KAFKA_OPENFLOW_ROLE TO ROLE OPENFLOW_ADMIN;

SET CURRENT_USER = (SELECT CURRENT_USER());   
GRANT ROLE KAFKA_OPENFLOW_ROLE TO USER IDENTIFIER($CURRENT_USER);


-- Step 2: Create Schema for Network Rules
-- ----------------------------------------------------------------------------

USE ROLE KAFKA_OPENFLOW_ROLE;
USE DATABASE KAFKA_OPENFLOW_DB;

-- Create schema for network rules
CREATE SCHEMA IF NOT EXISTS KAFKA_OPENFLOW_DB.NETWORKS;

-- Note: Do NOT create the application logs table here. Openflow will
-- automatically create tables based on Kafka topic names during ingestion.
-- By default, the table name will match the Kafka topic name (e.g., "application_logs").

-- Step 3: Create Network Rules
-- ----------------------------------------------------------------------------
-- IMPORTANT: Replace with your Kafka broker endpoint(s)
-- 
-- This quickstart works with any Kafka service:
-- - GCP Managed Kafka:    '34.123.45.67:9092' (public IP)
-- - AWS MSK:              'b-1.mycluster.kafka.us-east-1.amazonaws.com:9092'
-- - Confluent Cloud:      'pkc-xxxxx.us-east-1.aws.confluent.cloud:9092'
-- - Azure Event Hubs:     'myeventhub.servicebus.windows.net:9093'
-- - Self-hosted:          'kafka.mycompany.com:9092'
--
-- Note: Ensure network connectivity and firewall rules allow Snowflake access
-- For multiple brokers (recommended), include all broker endpoints in VALUE_LIST

CREATE OR REPLACE NETWORK RULE KAFKA_OPENFLOW_DB.NETWORKS.kafka_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'b0-pkc-41wq6.eu-west-2.aws.confluent.cloud:9092',
    'b1-pkc-41wq6.eu-west-2.aws.confluent.cloud:9092',
    'b2-pkc-41wq6.eu-west-2.aws.confluent.cloud:9092',
    'b3-pkc-41wq6.eu-west-2.aws.confluent.cloud:9092',
    'b4-pkc-41wq6.eu-west-2.aws.confluent.cloud:9092',
    'b5-pkc-41wq6.eu-west-2.aws.confluent.cloud:9092'
  );

-- Step 4: Create External Access Integration
-- ----------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION kafka_external_integration
  ALLOWED_NETWORK_RULES = (
    KAFKA_OPENFLOW_DB.NETWORKS.kafka_network_rule
  )
  ENABLED = TRUE
  COMMENT = 'Openflow SPCS runtime access for Kafka connector';

-- Grant usage to runtime role
GRANT USAGE ON INTEGRATION kafka_external_integration TO ROLE KAFKA_OPENFLOW_ROLE;

-- Step 5: Verify Setup
-- ----------------------------------------------------------------------------

-- Verify all objects were created
SHOW DATABASES LIKE 'KAFKA_OPENFLOW_DB';
SHOW SCHEMAS IN DATABASE KAFKA_OPENFLOW_DB;
SHOW WAREHOUSES LIKE 'KAFKA_OPENFLOW_WH';
SHOW NETWORK RULES IN SCHEMA KAFKA_OPENFLOW_DB.NETWORKS;
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'kafka_external_integration';

-- Switch to runtime role for subsequent operations
USE ROLE KAFKA_OPENFLOW_ROLE;
USE DATABASE KAFKA_OPENFLOW_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE KAFKA_OPENFLOW_WH;

-- ============================================================================
-- Setup Complete!
-- 
-- Next Steps:
-- 1. Update the kafka_network_rule VALUE_LIST with your actual Kafka broker endpoints
-- 2. Set up Openflow SPCS runtime (if not already done)
-- 3. Attach kafka_external_integration to your Openflow runtime
-- 4. Configure the Kafka connector in Openflow Canvas
-- 5. The connector will automatically create the table (e.g., application_logs)
--    based on your Kafka topic name
-- ============================================================================

SELECT 'Snowflake setup completed successfully!' AS STATUS;

