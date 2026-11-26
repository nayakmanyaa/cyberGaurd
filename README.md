# CyberGuard – Distributed Network Threat Detection and Graph-Based Investigation System

CyberGuard is a batch-mode cybersecurity analytics project that processes historical network traffic records (starting with **CIC-IDS2017**), detects explainable suspicious communication patterns using **PySpark DataFrame API** and **NumPy**, stores alerts and investigation records in **MongoDB**, models IP relationships in **Neo4j** (Cypher), and surfaces findings through a simple 3-page **Streamlit** + **Plotly** interface.

> **Current Phase:** Phase 2 — Environment setup, modular directory structure, configuration management, and PySpark verification. Threat detection, database ingestion, dataset download, and UI pages are intentionally deferred to later phases.

---

## Project Structure

```text
cyberguard/
├── data/
│   ├── raw/          # Raw CIC-IDS2017 CSVs (excluded from Git)
│   ├── processed/    # Processed Parquet outputs (excluded from Git)
│   └── sample/       # Small sample fixtures
├── config/           # Environment settings & PySpark SparkSession builder
├── ingestion/        # Batch ingestion (upcoming phase)
├── processing/       # PySpark DataFrame preprocessing (upcoming phase)
├── detection/        # Explainable threat rules & anomaly scoring (upcoming phase)
├── graph/            # Neo4j Cypher graph builders & queries (upcoming phase)
├── analytics/        # MongoDB alert storage & summaries (upcoming phase)
├── dashboard/        # 3-page Streamlit + Plotly UI (upcoming phase)
├── tests/            # Pytest test suite
├── scripts/          # Verification and batch pipeline scripts
├── Pipfile           # Pipenv dependency manifest
├── .env.example      # Environment variable template
├── .gitignore        # Excludes .env, raw/processed datasets, caches
└── README.md
```

---

## Prerequisites

- **Python**: 3.13 (Windows host) or 3.12+ (WSL2 Ubuntu 24.04)
- **Java**: JDK 17 (`JAVA_HOME` pointing to JDK 17)
- **Pipenv**: Virtual environment and dependency manager
- **MongoDB 8.0+** and **Neo4j 5.x / Neo4j Desktop 2** (configured now via `.env`; integrations implemented in a later phase)

