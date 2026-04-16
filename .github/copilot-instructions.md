# Copilot Project Instructions — AgAnalytics

## Project Purpose

This is a **presales demo** for a large North American greenhouse agriculture company. The demo showcases Microsoft Fabric's full platform capabilities — Real-Time Intelligence, OneLake, Direct Lake, Data Activator, ML, KQL Graph Semantics, and Fabric Agents — to win a Fabric engagement.

**Target audience:** C-level and VP-level executives at the customer, plus their technical evaluation team. The demo must be visually impressive, data-realistic, and tell a compelling story about moving from reactive to predictive operations.

**Critical rule:** The fictional company is called **"Cox Farms AgriTech"** internally, but in ALL code, config, UI labels, documentation, and generated data, use the fictional brand names:
- **BrightHarvest Greens** — the US leafy greens greenhouse (Rochelle, Illinois)
- **Mucci Valley Farms** — the Canadian vine crop greenhouse (Kingsville, Ontario)
- **AgriTech Analytics** — the parent company umbrella name in dashboards

Do NOT reference the real customer name ("Cox Farms", "Cox Enterprises", "BrightFarms", or "Mucci Farms") anywhere in code, data, or UI. The demo should feel like it *could* be their data without naming them directly.

---

## Architecture Overview

```
Data Generator (Python) → Azure Event Hub → Fabric Eventstream → Eventhouse (KQL)
                                                                      ↓
Historical Parquet → OneLake (Bronze/Silver/Gold) ← Lakehouse Notebooks
                                                         ↓
                              Direct Lake Semantic Model → Power BI Exec Dashboard
                                                         ↓
                                                   Fabric Data Agents
```

### Key Components
- **2 greenhouses**, 2 countries (US + Canada), **16 grow zones** (8 each), **8 crops**
- **Real-time sensor streaming** at 30-second intervals to Eventhouse
- **2 years of historical seed data** (Apr 2024 – Apr 2026) in OneLake Delta tables
- **4 anomaly scenarios** triggerable live during demo
- **Direct Lake semantic model** over Gold-layer Delta tables (zero-copy, no Import refresh)
- **KQL Graph Semantics** ontology for equipment dependency and blast-radius analysis
- **2 ML models**: yield prediction (LightGBM) + anomaly classifier (Random Forest)

---

## Tech Stack

- **Python 3.11+** for data generator (numpy, pandas, pyarrow, azure-eventhub, click)
- **Microsoft Fabric** — Eventhouse, Eventstream, Lakehouse, Data Activator, Data Science, Power BI
- **KQL** for real-time queries and anomaly detection (`series_decompose_anomalies`)
- **PySpark** for Lakehouse notebooks (Bronze → Silver → Gold medallion)
- **DAX** for semantic model measures
- **TMDL** for semantic model definition

---

## Microsoft Fabric MCP Tools Available in This Session

When building this project, leverage these MCP tools directly — they are live-connected to Fabric:

### Fabric Core MCP Tools (Azure MCP Server)

| MCP Tool Prefix | Capabilities | Use For |
|-----------------|-------------|---------|
| `fabric_mcp-core_*` | Create workspace items (Lakehouse, Notebook, etc.) | Provisioning Fabric items |
| `fabric_mcp-onelake_*` | List/create/upload/download files and directories in OneLake; list tables and namespaces | Uploading historical Parquet data, managing Delta tables, exploring OneLake storage |
| `fabric_mcp-docs_*` | Retrieve API specs, best practices, item definitions, workload docs | Looking up Fabric API schemas and best practices before generating code |

### Real-Time Intelligence MCP Tools (RTI MCP Server)

| MCP Tool Prefix | Capabilities | Use For |
|-----------------|-------------|---------|
| `fabric-rti-mcp-kusto_*` | Execute KQL queries, management commands, describe databases, list entities, sample data, ingest inline CSV, graph queries | Querying Eventhouse, creating tables, running anomaly detection KQL, graph semantics |
| `fabric-rti-mcp-eventstream_*` | Create/manage Eventstreams, add sources/destinations, build definitions | Setting up the real-time ingestion pipeline from Event Hub to Eventhouse |
| `fabric-rti-mcp-activator_*` | Create triggers, list Activator artifacts | Configuring Data Activator alert rules |
| `fabric-rti-mcp-map_*` | Create/manage Map items | Visual mapping if needed |

### Power BI Semantic Modeling MCP Tools

| MCP Tool Prefix | Capabilities | Use For |
|-----------------|-------------|---------|
| `powerbi-modeling-mcp-connection_*` | Connect to Power BI Desktop, Fabric service, Analysis Services | Connecting to the Direct Lake semantic model |
| `powerbi-modeling-mcp-database_*` | Create, update, import/export TMDL, deploy to Fabric | Deploying the semantic model |
| `powerbi-modeling-mcp-table_*` | Create/update/delete tables, get schema, export TMDL | Building the star schema fact and dimension tables |
| `powerbi-modeling-mcp-column_*` | Manage columns | Defining column properties, data types, formatting |
| `powerbi-modeling-mcp-measure_*` | Create/update/delete/move DAX measures | Building the DAX measures library (yield, energy, supply chain KPIs) |
| `powerbi-modeling-mcp-relationship_*` | Create/manage relationships | Defining the 14 star-schema relationships |
| `powerbi-modeling-mcp-partition_*` | Manage partitions, refresh | Direct Lake partition management |
| `powerbi-modeling-mcp-dax_query_*` | Execute/validate DAX queries | Testing DAX measures |
| `powerbi-modeling-mcp-perspective_*` | Manage perspectives | Creating focused views for different user roles |
| `powerbi-modeling-mcp-security_role_*` | Manage RLS roles and permissions | Row-level security if needed |
| `powerbi-modeling-mcp-calculation_group_*` | Manage calculation groups | Time intelligence calculation groups |
| `powerbi-modeling-mcp-model_*` | Get/update/refresh model, export TMDL | Model-level operations |
| `powerbi-modeling-mcp-named_expression_*` | Manage M expressions and parameters | Power Query parameters |
| `powerbi-modeling-mcp-function_*` | Manage model functions | Custom functions |
| `powerbi-modeling-mcp-user_hierarchy_*` | Manage hierarchies | Date and geography hierarchies |
| `powerbi-modeling-mcp-culture_*` | Manage cultures/translations | Localization if needed |
| `powerbi-modeling-mcp-trace_*` | Trace and performance diagnostics | Debugging query performance |
| `powerbi-modeling-mcp-transaction_*` | Transaction management | Batch operations |

### Architecture Diagram Tools

| MCP Tool | Use For |
|----------|---------|
| `fabric-arch-slides-*` | Generate architecture diagram PowerPoint slides with Microsoft icons (Azure, Fabric, Power Platform, D365, M365) |
| `fabric-arch-slides-search_icons` | Find icon IDs for architecture diagrams |
| `fabric-arch-slides-render_template` | Use pre-built templates (medallion_lakehouse, rti_pipeline, end_to_end, etc.) |
| `fabric-arch-slides-create_architecture_slide` | Create custom architecture diagrams |

### Azure Infrastructure MCP Tools (for Event Hub, storage, etc.)

| MCP Tool Prefix | Use For |
|-----------------|---------|
| `azure_mcp-eventhubs` | Managing the Azure Event Hub namespace and hubs for real-time ingestion |
| `azure_mcp-storage` | Managing Azure Storage if needed for staging |
| `azure_mcp-monitor` | Azure Monitor queries for infrastructure diagnostics |
| `azure_mcp-keyvault` | Secrets management for connection strings |
| `azure_mcp-subscription_list` | Discovering Azure subscriptions |
| `azure_mcp-group_*` | Resource group management |

---

## Skills for Fabric (GitHub: microsoft/skills-for-fabric)

Beyond the MCP tools above, the project should be compatible with the **Skills for Fabric** agent skills from `github.com/microsoft/skills-for-fabric`. These are first-party agent skills organized by persona:

### Authoring Skills
| Skill | Purpose |
|-------|---------|
| `sqldw-authoring-cli` | Author Warehouses, Lakehouse SQL Endpoints, Mirrored Databases |
| `spark-authoring-cli` | Build Fabric Spark and Data Engineering workflows |
| `eventhouse-authoring-cli` | Manage KQL tables, ingestion, policies, and functions |
| `powerbi-authoring-cli` | Create and deploy Power BI semantic models |

### Consumption Skills
| Skill | Purpose |
|-------|---------|
| `sqldw-consumption-cli` | Query Warehouses and SQL Endpoints interactively |
| `spark-consumption-cli` | Analyze Lakehouse tables |
| `eventhouse-consumption-cli` | Run read-only KQL queries |
| `powerbi-consumption-cli` | Query semantic models, execute DAX |

### Cross-Workload Skills
| Skill | Purpose |
|-------|---------|
| `e2e-medallion-architecture` | End-to-end Bronze → Silver → Gold medallion pipeline |
| `check-updates` | Auto-check for skill updates |

### Agent Personas (from skills-for-fabric/agents/)
| Agent | Purpose |
|-------|---------|
| `FabricDataEngineer` | Medallion architectures, ETL/ELT, migration, data quality |
| `FabricAdmin` | Capacity, governance, security, cost, observability |
| `FabricAppDev` | Build applications on top of Fabric using Python, ODBC, XMLA, REST |

---

## Fabric Workloads Available via API (Post-FabCon Atlanta 2025)

The Fabric platform exposes these workload types via REST API and MCP — all are available for this demo if relevant:

### Core Analytics
- **Lakehouse** — Delta tables, Spark, OneLake storage
- **Warehouse** — T-SQL analytics warehouse
- **SQL Database** — OLTP database in Fabric
- **Eventhouse** — KQL-based real-time analytics (our primary RTI store)
- **KQL Database** — Individual database within an Eventhouse
- **SQL Endpoint** — SQL access layer over Eventhouse data (NEW — post-FabCon)

### Real-Time Intelligence
- **Eventstream** — Real-time event ingestion and routing
- **KQL Queryset** — Saved KQL queries
- **KQL Dashboard** — Real-time dashboards with KQL-powered tiles
- **Anomaly Detector** — Automated anomaly detection on Eventhouse tables (Preview)
- **Reflex / Activator** — Event-driven rules engine with automated actions
- **Map** — Geographic visualization items
- **Event Schema Set** — Schema registry for streaming events

### Data Science & AI
- **Notebook** — PySpark/Python notebooks with Copilot
- **ML Experiment** — MLflow experiment tracking
- **ML Model** — MLflow model registry
- **Data Agent** — Conversational AI over Lakehouse, Warehouse, Semantic Model, KQL DB, and **Ontology** (NEW)
- **Ontology** — Knowledge graph definitions for agent grounding (NEW — post-FabCon)
- **Operations Agent** — Agent for Fabric operational monitoring (NEW)
- **AI Functions** — Inline AI transforms in notebooks (NEW)
- **Environment** — Configurable Spark environments
- **Spark Job Definition** — Reusable Spark job configurations

### Data Integration
- **Data Pipeline** — Data Factory orchestration pipelines
- **Dataflow** — Power Query dataflows (Gen2)
- **Datamart** — Self-service data mart with auto-generated semantic model
- **Copy Job** — Simplified data copy operations
- **Apache Airflow Job** — Managed Airflow DAGs (NEW)
- **Mounted Data Factory** — Existing ADF integration (NEW)
- **Variable Library** — Shared variables across pipelines (NEW)

### Business Intelligence
- **Semantic Model** — Power BI semantic model (Direct Lake, Import, DirectQuery)
- **Report** — Power BI reports
- **Paginated Report** — Pixel-perfect reports
- **Dashboard** — Power BI classic dashboards
- **Real-Time Dashboard** — KQL-powered live dashboards

### Graph & Knowledge
- **Graph Model** — KQL graph schema definitions (NEW — post-FabCon)
- **Graph Query Set** — Saved graph queries (NEW)
- **GraphQL API** — GraphQL endpoint over Fabric data (NEW)
- **Digital Twin Builder** — Digital twin model definitions (NEW)
- **Digital Twin Builder Flow** — Digital twin data flows (NEW)

### Governance & Platform
- **Mirrored Database** — Real-time mirroring from external databases
- **Mirrored Warehouse** — Mirrored data warehouse
- **Mirrored Azure Databricks Catalog** — Unity Catalog mirroring (NEW)
- **Snowflake Database** — Snowflake mirroring (NEW)
- **Cosmos DB Database** — Cosmos DB mirroring
- **User Data Function** — Custom functions triggered by Activator (NEW)
- **Warehouse Snapshot** — Point-in-time warehouse snapshots (NEW)
- **Real-Time Intelligence** — Meta-workload encompassing Eventhouse, Eventstream, KQL, Activator

---

## Key Fabric Features to Showcase (Post-FabCon Atlanta 2025)

### Eventhouse Convergence (NEW)
- **SQL Endpoint on Eventhouse** — Query event data with familiar T-SQL, not just KQL
- **Notebook integration** — Open notebooks directly against Eventhouse databases
- **Data Agent on Eventhouse** — Agents can reason over live + historical event data
- **Anomaly Detection to Activator** — Seamless pipeline from detection to automated action
- **Capacity Scheduler** — Schedule different capacity levels for peak/off-peak (cost optimization)

### Ontology-Powered Agents (NEW)
- **Ontology item type** — Define knowledge graphs that ground agent responses
- **Data Agents with Ontology source** — Agents can use ontology as a data source alongside Lakehouse, Warehouse, Semantic Model, and KQL DB
- Up to 5 data sources per agent (any combination)

### Graph Semantics in KQL (GA)
- **Persistent graph models** with schema definitions (node labels, edge labels, properties)
- **Graph snapshots** for point-in-time analysis
- **`graph-match` operator** for pattern matching, path traversal, blast-radius queries
- **Variable-length paths** (`*1..5`) for dependency chain analysis

### Direct Lake on OneLake (GA)
- **Zero-copy analytics** — reads Delta Parquet files directly, no Import duplication
- **Framing** — metadata refresh in seconds, not minutes
- **Composite models** — combine Direct Lake with Import tables
- **Automatic updates** — dashboard sees new data immediately after pipeline writes

### Data Activator / Reflex (GA)
- **Stateful rules** — BECOMES, INCREASES, DECREASES, EXIT_RANGE, heartbeat absence
- **Actions** — trigger Notebooks, Pipelines, Spark Jobs, Power Automate, Teams, Email
- **Activator as orchestrator** — Detection → Transformation, Detection → Notification, Detection → Model Scoring
- **Preview/impact estimation** — see how often a rule would fire on historical data before activating

---

## Demo Data Model Summary

### Greenhouses
| ID | Name | Location | Zones | Crops |
|----|------|----------|-------|-------|
| `brightharvest` | BrightHarvest Greens | Rochelle, IL, USA | 8 (BH-Z01..BH-Z08) | Baby Spinach, Romaine, Arugula, Basil |
| `mucci-valley` | Mucci Valley Farms | Kingsville, ON, Canada | 8 (MV-Z01..MV-Z08) | Cocktail Tomato, Bell Pepper, Mini Cucumber, Strawberry |

### Sensor Types (per zone, every 30 seconds)
`air_temperature`, `air_humidity`, `co2_level`, `par_light`, `substrate_temperature`, `substrate_moisture`, `substrate_ec`, `substrate_ph`, `water_flow_rate`, `vpd`

### Live Trigger Scenarios
| Command | What It Does | Detection Time |
|---------|-------------|----------------|
| `aganalytics trigger --scenario hvac-failure` | Boiler offline, temp drops | ~90 seconds |
| `aganalytics trigger --scenario nutrient-drift` | pH slowly rises | ~3-4 minutes |
| `aganalytics trigger --scenario irrigation-failure` | Pump fails, moisture drops | ~2 minutes |
| `aganalytics trigger --scenario cold-chain-break` | Truck temp rises | ~5 minutes |

---

## Code Style & Conventions

- **Python:** Use type hints, dataclasses, f-strings. Format with `ruff`. Test with `pytest`.
- **KQL:** Use descriptive variable names, comment each query block, use `let` for reusable expressions.
- **DAX:** Prefix measures with domain (`Yield:`, `Energy:`, `Supply:`, `Anomaly:`). Use `VAR` for readability.
- **File naming:** snake_case for Python, kebab-case for CLI commands, PascalCase for KQL table names.
- **Parquet:** Partition by `year/month` for fact tables. Use Delta format for all OneLake tables.
- **Config:** All magic numbers in `config.py`. No hardcoded connection strings — use environment variables.

---

## What "Done" Looks Like

A successful demo has these artifacts working end-to-end:

1. ✅ `aganalytics generate-historical` produces 2 years of realistic Parquet data
2. ✅ Data uploaded to OneLake Bronze → transformed through Silver → Gold via notebooks
3. ✅ Direct Lake semantic model connected to Gold tables, DAX measures working
4. ✅ Executive dashboard shows yield trends, YoY comparisons, sustainability metrics
5. ✅ `aganalytics stream` sends live telemetry to Event Hub → Eventstream → Eventhouse
6. ✅ RTI Dashboard shows live zone status, sensor trends, equipment state
7. ✅ `aganalytics trigger --scenario hvac-failure` causes visible anomaly on RTI dashboard within 90 seconds
8. ✅ Anomaly Detection flags it, Activator fires Teams notification
9. ✅ KQL graph query shows blast radius and runbook actions
10. ✅ Fabric Data Agent answers "What happened in Zone 5?" with incident summary
