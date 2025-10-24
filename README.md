# Data Engineering Capstone Project

A comprehensive end-to-end data engineering pipeline using the UCI Online Retail dataset, demonstrating modern data engineering practices with industry-standard tools and methodologies.

## Project Overview

This project implements a production-grade data pipeline covering the complete modern data stack:

- ✅ **Milestone 1**: Development environment setup and data exploration
- ✅ **Milestone 2**: Medallion Architecture implementation (Bronze/Silver/Gold layers)
- ✅ **Milestone 3**: Data transformation with dbt (Data Build Tool)
- 🔄 **Milestone 4**: Workflow orchestration with Apache Airflow
- 🔄 **Milestone 5**: Data quality testing with Great Expectations
- 🔄 **Milestone 6**: Stream processing with Apache Kafka
- 🔄 **Milestone 7**: Big data processing with Apache Spark
- 🔄 **Milestone 8**: Analytics and visualization with Tableau/Power BI

## Architecture Overview

### Medallion Architecture

Our data architecture follows the industry-standard Medallion (Bronze → Silver → Gold) pattern:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  BRONZE LAYER   │───▶│  SILVER LAYER   │───▶│   GOLD LAYER    │
│                 │    │                 │    │                 │
│ • Raw data      │    │ • Cleaned data  │    │ • Star schema   │
│ • Audit trails  │    │ • Business rules│    │ • Aggregations  │
│ • Data lineage  │    │ • Validation    │    │ • Analytics     │
│ • Immutable     │    │ • Conformed     │    │ • BI-ready      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Technology Stack

| Layer | Tool/Technology | Purpose |
|-------|----------------|---------|
| **Source** | UCI ML Repository | Online Retail dataset (0.5M+ records) |
| **Ingestion** | Python + SQLAlchemy | Programmatic data download and loading |
| **Storage** | PostgreSQL 13+ | Relational database with schema separation |
| **Transformation** | dbt (Data Build Tool) | SQL-based ELT transformations with testing |
| **Orchestration** | Apache Airflow | Workflow scheduling and monitoring |
| **Quality** | Great Expectations | Data validation and quality checks |
| **Streaming** | Apache Kafka | Real-time data ingestion |
| **Big Data** | Apache Spark | Distributed data processing |
| **Visualization** | Tableau Public / Power BI | Business intelligence dashboards |
| **Version Control** | Git + GitHub/GitLab | Code versioning with nbdime for notebooks |

### Database Architecture

#### Bronze Layer (`bronze` schema)
- **Purpose**: Raw data ingestion with complete audit trail
- **Tables**:
  - `online_retail_raw`: Source data with metadata columns (source_file_name, load_timestamp, record_hash, etc.)
  - `ingestion_log`: Job tracking with execution metrics and error handling

#### Silver Layer (`silver` schema)
- **Purpose**: Cleaned, validated, and standardized data
- **Models** (dbt):
  - `stg_bronze__online_retail`: Type-cast and cleaned staging view
  - `int_customer_transactions`: Customer aggregation logic
  - `int_product_metrics`: Product performance calculations
- **Features**: Data quality flags, duplicate removal, business rule enforcement

#### Gold Layer (`gold` schema)
- **Purpose**: Star schema for analytics and BI consumption
- **Dimensional Model**:
  - `fact_sales`: Transaction fact table (0.5M+ rows)
  - `dim_customer`: Customer dimension with RFM segmentation
  - `dim_product`: Product dimension with categorization
  - `dim_date`: Calendar dimension (2009-2012)
  - `dim_geography`: Country-level geographic attributes

## Dataset Information

**Source**: UCI Machine Learning Repository - Online Retail Dataset

**Statistics**:
- **Records**: 541,909 transactions
- **Time Period**: December 2009 - December 2011
- **Customers**: 5,942 unique customers across 40+ countries
- **Products**: 4,070+ unique stock codes
- **Total Revenue**: £9.7M+

**Business Context**: E-commerce transactions from a UK-based online retailer specializing in unique all-occasion gifts, primarily serving wholesale customers.

## Project Structure

```
data-engineering-capstone/
├── data/
│   ├── raw/                  # Raw source files (CSV, Parquet)
│   ├── staging/              # Intermediate processing artifacts
│   └── warehouse/            # Exported analytical datasets
├── notebooks/
│   ├── exploration/          # Data profiling and EDA
│   └── analysis/             # Business insights and reporting
├── src/
│   ├── ingestion/            # Bronze layer ETL pipelines
│   ├── transformation/       # Business logic (deprecated - moved to dbt)
│   └── utils/                # Database connections and helpers
├── retail_transform/         # dbt project root
│   ├── models/
│   │   ├── staging/          # Bronze → Silver transformations
│   │   ├── intermediate/     # Complex business logic
│   │   └── marts/            # Gold layer dimensional models
│   ├── macros/               # Reusable SQL functions
│   ├── tests/                # Custom data quality tests
│   └── dbt_project.yml       # dbt configuration
├── config/                   # Database and pipeline configurations
├── logs/                     # Application and ETL logs
├── scripts/                  # Utility scripts (setup, validation)
├── tests/                    # Python unit and integration tests
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

## Getting Started

### Prerequisites

- **Python**: 3.9 or higher
- **PostgreSQL**: 13 or higher
- **Git**: 2.30 or higher
- **Operating System**: Linux, macOS, or Windows with WSL

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/data-engineering-capstone.git
   cd data-engineering-capstone
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/macOS
   # OR
   venv\Scripts\activate     # Windows
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   ```bash
   echo "POSTGRES_PASSWORD=your_secure_password" > .env
   ```

5. **Initialize database schemas**
   ```bash
   # Create medallion schemas (bronze, silver, gold)
   psql -U postgres -d online_retail_dev -f scripts/create_medallion_schemas.sql
   psql -U postgres -d online_retail_dev -f scripts/create_bronze_tables.sql
   psql -U postgres -d online_retail_dev -f scripts/create_silver_tables.sql
   psql -U postgres -d online_retail_dev -f scripts/create_gold_tables.sql
   ```

6. **Configure dbt profile**
   ```bash
   # Edit ~/.dbt/profiles.yml with your database credentials
   cd retail_transform
   dbt debug  # Verify connection
   ```

### Running the Pipeline

#### Step 1: Data Ingestion (Bronze Layer)
```bash
# Download UCI dataset and load to bronze layer
python src/ingestion/download_data.py
python src/ingestion/bronze_ingestion.py

# Verify ingestion
psql -U postgres -d online_retail_dev -c "SELECT COUNT(*) FROM bronze.online_retail_raw;"
```

#### Step 2: Data Transformation (Silver & Gold Layers)
```bash
cd retail_transform

# Run all dbt models (staging → intermediate → marts)
dbt run

# Run data quality tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve  # Opens browser at localhost:8080
```

#### Step 3: Validation
```bash
# Run end-to-end validation
cd ..
psql -U postgres -d online_retail_dev -f scripts/validate_pipeline.sql
```

## Key Features

### 🏗️ Medallion Architecture
- **Bronze Layer**: Immutable raw data with complete audit trail (source tracking, load timestamps, record hashing)
- **Silver Layer**: Cleaned and validated data with business rules applied via dbt transformations
- **Gold Layer**: Star schema dimensional model optimized for analytical queries

### 🔧 dbt Transformation Framework
- **Modular Design**: Staging → Intermediate → Marts layering pattern
- **Version Control**: SQL transformations tracked in Git with full lineage
- **Testing**: 34+ automated data quality tests (uniqueness, referential integrity, business rules)
- **Documentation**: Auto-generated data dictionary with interactive lineage graphs
- **Incremental Loading**: Efficient processing of only new/changed data

### 📊 Dimensional Modeling
- **Star Schema**: Optimized for BI tools and analytical queries
- **Customer Segmentation**: RFM (Recency, Frequency, Monetary) analysis
- **Product Categories**: Rule-based categorization from product descriptions
- **Time Intelligence**: Comprehensive date dimension with fiscal periods and business calendar

### 🎯 Data Quality
- **Source Tests**: 10+ tests on bronze layer data integrity
- **Transformation Tests**: 20+ tests on staging and intermediate models
- **Referential Integrity**: Custom tests ensuring valid foreign key relationships
- **Business Rules**: Automated validation of data ranges, accepted values, and calculations

### 🔍 Data Lineage
- **End-to-End Traceability**: Track data from source file through all transformation layers
- **Metadata Capture**: Source file paths, load timestamps, batch IDs, and record hashes
- **Visual Lineage**: Interactive DAG visualization in dbt documentation
- **Audit Trail**: Comprehensive logging of all pipeline executions

## Development Workflow

### Git Strategy

This project follows **GitFlow** branching strategy:

- `main`: Production-ready releases with milestone tags (v1.0, v2.0, v3.0)
- `develop`: Integration branch for ongoing development
- `feature/*`: Individual feature development
- `milestone/*`: Milestone-specific implementations

### Commit Message Convention

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`

**Examples**:
- `feat(dbt): implement customer dimension with RFM segmentation`
- `fix(ingestion): correct record hash calculation for duplicate detection`
- `docs(readme): update architecture diagram with dbt layer`
- `test(silver): add referential integrity tests for fact_sales`

### Code Review Checklist

- [ ] All dbt models documented with descriptions
- [ ] Data quality tests added for new models
- [ ] Code follows naming conventions (staging: `stg_`, intermediate: `int_`, marts: `dim_`/`fact_`)
- [ ] Commit messages follow conventional format
- [ ] Pipeline runs successfully end-to-end
- [ ] Documentation updated (README, inline comments)

## Performance & Scalability

### Database Optimization
- **Indexing**: Strategic indexes on foreign keys, date columns, and frequently filtered fields
- **Materialization**: Views for staging (fresh data), tables for marts (query performance)
- **Incremental Models**: Only process new/changed records in fact tables
- **Connection Pooling**: Efficient database connection management via SQLAlchemy

### Pipeline Efficiency
- **Batch Processing**: Configurable batch sizes (10,000 rows default) for memory management
- **Parallel Execution**: dbt runs models in parallel based on dependency graph (4 threads default)
- **Ephemeral Models**: Intermediate models as CTEs (not persisted) for reduced storage
- **Query Optimization**: Proper join strategies and filter pushdown in SQL transformations

## Testing Strategy

### dbt Tests (34+ automated tests)
- **Generic Tests**: `unique`, `not_null`, `accepted_values`, `relationships`
- **dbt_utils Tests**: `accepted_range`, `at_least_one`, `expression_is_true`
- **Custom Tests**: Referential integrity across fact-dimension relationships
- **Source Tests**: Data freshness and completeness checks on bronze layer

### Python Tests (Planned - Milestone 4+)
- Unit tests for ingestion pipeline components
- Integration tests for end-to-end data flow
- Performance tests for scalability validation

## Monitoring & Observability

### Logging
- **Application Logs**: `logs/bronze_ingestion.log` with structured formatting
- **dbt Logs**: `retail_transform/logs/dbt.log` with model execution details
- **Database Logs**: PostgreSQL logs for query performance analysis

### Metrics Tracking
- **Ingestion Metrics**: Records processed, execution time, throughput (records/second)
- **Data Quality Scores**: Tracked in `bronze.ingestion_log` and dbt test results
- **Pipeline Health**: Success/failure rates, test pass rates, data freshness

## Milestones Completed

### ✅ Milestone 1: Foundation Setup
- Development environment configuration (Python, PostgreSQL, Git, Jupyter)
- UCI Online Retail dataset download and initial exploration
- Project structure creation following data engineering best practices
- Database connection utilities and testing framework

### ✅ Milestone 2: Medallion Architecture
- Three-tier schema design (bronze, silver, gold) with proper separation
- Bronze layer ingestion pipeline with metadata tracking and audit trails
- Silver and gold table definitions with constraints and indexes
- Data lineage implementation (source tracking, batch IDs, record hashing)
- Comprehensive validation framework

### ✅ Milestone 3: dbt Transformation
- dbt project initialization with staging/intermediate/marts structure
- Source definitions with freshness checks on bronze layer
- Staging models for type casting and data cleaning (bronze → silver)
- Intermediate models for customer and product aggregations
- Dimensional models (3 dimensions + 1 fact table) in star schema
- 34+ automated data quality tests with custom referential integrity checks
- Interactive documentation with lineage visualization
- Custom macros for code reusability

## Upcoming Milestones

### 🔄 Milestone 4: Workflow Orchestration (Apache Airflow)
- DAG creation for automated pipeline execution
- Task dependencies and error handling
- Schedule-based and event-driven triggers
- Integration with dbt for transformation orchestration

### 🔄 Milestone 5: Data Quality (Great Expectations)
- Expectation suites for comprehensive data validation
- Data profiling and anomaly detection
- Quality dashboard and alerting
- Integration with Airflow for automated checks

### 🔄 Milestone 6-8: Advanced Topics
- Real-time streaming with Kafka
- Distributed processing with Spark
- BI dashboards and analytics

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature-name`)
3. Follow code style and naming conventions
4. Add tests for new functionality
5. Update documentation (README, dbt docs, inline comments)
6. Commit with conventional commit messages
7. Push and create a Pull Request

## Resources & References

### Official Documentation
- [dbt Documentation](https://docs.getdbt.com)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [UCI Online Retail Dataset](https://archive.ics.uci.edu/dataset/352/online+retail)

### Learning Materials
- [dbt Best Practices](https://docs.getdbt.com/best-practices)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)
- [Git Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact & Support

**Author**: [Gaurav Jain]  
**Email**: [garv.jain.jpr@gmail.com]  
**Project Repository**: [https://github.com/gauravjainjpr/data-engineering-capstone](https://github.com/gauravjainjpr/data-engineering-capstone)

**Issues & Questions**: Please open an issue on GitHub for bug reports, feature requests, or questions.

---

**Last Updated**: October 2025  
**Current Version**: v3.0 (Milestone 3 - dbt Transformation)  
**Next Release**: v4.0 (Milestone 4 - Airflow Orchestration)
