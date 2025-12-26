# Getting Started with Stream to Insight: Real-Time Kafka to Snowflake Pipelines with Openflow and Intelligent Log Agents
## Overview

Build a real-time streaming pipeline from Apache Kafka to Snowflake using Openflow. Stream application logs from Kafka to Snowflake with automatic schema detection, schema evolution, and perform powerful SQL analytics including semantic search with Snowflake Intellegent Agent.


## Repository Contents

```
.
├── README.md                      # This file
├── RPK_CLI_README.md              # Detailed RPK CLI setup guide
├── Taskfile.yml                   # Task runner for common operations
├── LICENSE.txt                    # Apache 2.0 license
├── pyproject.toml                 # Python project dependencies
├── .env.template                  # Environment variable template
├── sql/
│   ├── 1.snowflake_setup.sql     # Snowflake environment setup
│   ├── 2.verify_ingestion.sql    # Data ingestion verification
│   ├── 2a.verify_base_schema.sql # Verify base schema ingestion
│   ├── 2b.verify_schema_evolution.sql # Verify schema evolution
│   ├── 3.analytics_queries.sql   # Example analytics queries
│   ├── 4.cortex_search.sql       # Semantic search with Cortex Search
│   ├── 5.setup_alert.sql         # Setup Alert
│   └── 6.cleanup.sql             # Cleanup script
└── data/
    ├── sample_logs.json           # 50 base schema sample records
    ├── sample_logs_evolved.json   # 80 evolved schema sample records
    └── generate_logs.py           # Python log generator script
```


### 1. Clone and Setup

```bash
git clone https://github.com/snow2ocean/kafka-openflow-snowflake.git
cd kafka-openflow-snowflake
export KAFKA_OPENFLOW_REPO=$PWD
```

### 2. Follow the Quickstart Guide

**👉 [Complete Step-by-Step Instructions](https://www.youtube.com/@Snow2Ocean)**

The quickstart guide walks you through:

- Setting up Kafka and rpk CLI
- Configuring Snowflake (database, network rules, External Access Integration)
- Deploying Openflow SPCS runtime
- Configuring the Kafka connector with automatic schema evolution
- Generating and streaming logs
- Running analytics and Cortex Search queries

## Common Tasks

### Generate Sample Files (No Kafka Required)

```bash
task generate-samples   # Generate both base and evolved sample files
task generate-base      # Generate 50 base schema records
task generate-evolved   # Generate 80 evolved schema records
```

### Test Kafka Connection

```bash
# Setup environment
cp .env.template .env   # Edit with your Kafka credentials
task test-kafka         # Verify connection
```

### Produce Logs to Kafka

```bash
# Using Python generator
task produce-base       # Produce 50 base schema logs
task produce-evolved    # Produce 80 evolved schema logs

# Using rpk (faster)
task rpk-produce-base
task rpk-produce-evolved

# Complete demo workflow
task demo-full          # Run Phase 1 + Phase 2
```

### Kafka Topic Management

```bash
task kafka-create-topic      # Create application-logs topic
task kafka-topics            # List all topics
task kafka-cluster-info      # Show cluster information
```

## Prerequisites

- **Snowflake Account**: Enterprise account with Openflow SPCS enabled
- **Kafka Cluster**: Access to Kafka (Confluent Cloud, AWS MSK, GCP Managed Kafka, or self-hosted)
- **Redpanda CLI (rpk)**: For Kafka operations ([install guide](https://docs.redpanda.com/current/get-started/rpk-install/))
  - 📖 See [RPK_CLI_README.md](RPK_CLI_README.md) for detailed setup with Confluent Cloud
- **Python 3.7+**: Optional, for the log generator script
- **Task Runner**: Optional, but recommended for simplified command execution

## Sample Data

**`data/sample_logs.json`** (50 records):

- Base schema with 11 fields (timestamp, level, service, host, message, etc.)
- Web API requests, authentication, database operations, payments

**`data/sample_logs_evolved.json`** (80 records):

- Evolved schema with 29 fields
- Includes additional fields: region, trace_id, auth_method, currency, payment_method, and more
- Contains system metrics (memory_percent, disk_usage_percent) for Cortex Search examples

**`data/generate_logs.py`**:

- Generate custom log files: `python generate_logs.py --count 100 --output my_logs.json`
- Test Kafka connection: `python generate_logs.py --test-connection`
- Produce to Kafka: `python generate_logs.py --count 100`
- See `--help` for all options

## SQL Scripts

All SQL scripts are referenced in the quickstart guide:

- **`1.snowflake_setup.sql`** - Create role, database, warehouse, network rules, External Access Integration
- **`2.verify_ingestion.sql`** - Verify data ingestion and check record counts
- **`2a.verify_base_schema.sql`** - Validate base schema (11 fields)
- **`2b.verify_schema_evolution.sql`** - Verify evolved schema (29 fields) and automatic column detection
- **`3.analytics_queries.sql`** - Advanced analytics examples (log analysis, time-series, performance metrics)
- **`4.cortex_search.sql`** - Semantic search queries with Cortex Search
- **`5.setup_alert.sql`** - Setup Alert
- **`5.cleanup.sql`** - Clean up all Snowflake resources


## License

Copyright (c) 2025 Snowflake Inc. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE.txt](LICENSE.txt) for details.
