# Data Engineering Capstone Project

A comprehensive end-to-end data engineering pipeline using the UCI Online Retail dataset.

## Project Overview

This project demonstrates modern data engineering practices by building a complete pipeline that:
- ✅ **Milestone 1**: Foundation setup with exploration
- ✅ **Milestone 2**: Medallion Architecture with Bronze/Silver/Gold layers
- 🔄 **Milestone 3**: Data transformation with dbt (Coming Next)
- 🔄 **Milestone 4**: Workflow orchestration with Apache Airflow
- 🔄 **Milestone 5**: Data quality testing with Great Expectations
- 🔄 **Milestone 6**: Stream processing with Apache Kafka
- 🔄 **Milestone 7**: Big data processing with Apache Spark
- 🔄 **Milestone 8**: Analytics and visualization

## Architecture Overview

### Medallion Architecture

Our data architecture follows the industry-standard Medallion (Bronze/Silver/Gold) pattern:

┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ BRONZE LAYER │───▶│ SILVER LAYER │───▶│ GOLD LAYER │
│ │ │ │ │ │
│ - Raw data │ │ - Cleaned data │ │ - Star schema │
│ - Audit trails │ │ - Business rules│ │ - Aggregations │
│ - Data lineage │ │ - Validation │ │ - Analytics │
└─────────────────┘ └─────────────────┘ └─────────────────┘

### Database Schemas

#### Bronze Layer (`bronze` schema)
- `retail_raw`: Raw ingested data with audit columns
- `load_metadata`: Tracks all ingestion jobs and their status

#### Silver Layer (`silver` schema)  
- `retail_cleaned`: Cleaned data with business rules applied
- `data_quality_metrics`: Quality metrics tracking

#### Gold Layer (`gold` schema)
- Star Schema for Analytics:
  - `fact_sales`: Sales transactions (fact table)
  - `dim_date`: Date dimension with business calendar
  - `dim_product`: Product catalog with categories
  - `dim_customer`: Customer information and segments
  - `dim_geography`: Geographic reference data

## Dataset

**Source**: UCI Machine Learning Repository - Online Retail Dataset
**Records**: 541,909 transactions
**Time Period**: December 2009 to December 2011
**Description**: Transactions from a UK-based online retail company specializing in unique all-occasion gifts.

### Data Quality Metrics
- **Completeness**: 97.4566% (measured across all critical fields)
- **Validity**: 99.9998% (business rule compliance)  
- **Consistency**: 100.0% (duplicate detection and standardization)
- **Overall Quality Score**: 99.1521%

## Project Structure

data-engineering-capstone/
├── data/
│ ├── raw/ # Raw, unprocessed data files
│ ├── staging/ # Intermediate processed data and profiles
│ └── warehouse/ # Final, analysis-ready data exports
├── notebooks/
│ ├── exploration/ # Data exploration and analysis notebooks
│ ├── transformation/# Data transformation development
│ └── analysis/ # Business analysis and insights
├── src/
│ ├── database/ # Database schema management
│ ├── ingestion/ # Data ingestion pipelines
│ ├── quality/ # Data quality and profiling tools
│ ├── transformation/# Data transformation logic (dbt models)
│ ├── utils/ # Shared utilities and helpers
│ └── validation/ # Data validation and testing
├── config/ # Configuration files
├── logs/ # Application and pipeline logs
├── docs/ # Project documentation
├── tests/ # Unit and integration tests
└── scripts/ # Utility and deployment scripts

## Getting Started

### Prerequisites
- Python 3.9+
- PostgreSQL 13+
- Git 2.30+

### Setup
1. Clone the repository
git clone https://github.com/yourusername/data-engineering-capstone.git
cd data-engineering-capstone
2. Create virtual environment: `python -m venv venv`
3. Activate virtual environment: `source venv/bin/activate` (Linux/macOS) or `venv\Scripts\activate` (Windows)
4. Install dependencies: `pip install -r requirements.txt`
5. Configure database connection in `config/database.yml`
    a. Copy and edit configuration: `cp config/database.yml.example config/database.yml`
6. Edit .env file with your database password: `echo "POSTGRES_PASSWORD=your_secure_password" > .env`

**Initialize database**
`python src/database/schema_design.py`

**Run data ingestion** 
`python src/ingestion/download_data.py`
`python src/ingestion/retail_ingestion_pipeline.py`

**Verify setup**
`python src/validation/verify_ingestion.py`

## Key Features

### 🏗️ **Medallion Architecture**
- **Bronze Layer**: Raw data ingestion with full audit trail
- **Silver Layer**: Cleaned data with business rules and validation  
- **Gold Layer**: Star schema optimized for analytics

### 🔍 **Data Quality Framework**
- Comprehensive data profiling and quality assessment
- Automated validation rules and constraints
- Quality metrics tracking and monitoring
- Business rule enforcement

### 🚀 **Production-Ready Features**
- Robust error handling and logging
- Batch processing with configurable batch sizes
- Retry logic for transient failures  
- Comprehensive monitoring and alerting
- Data lineage and audit trails

### 📊 **Analytics Optimization**
- Star schema design for fast queries
- Proper indexing strategy
- Date dimension with business calendar
- Pre-computed metrics and aggregations

## Development Workflow

This project follows GitFlow branching strategy:
- `main`: Production-ready code with milestone tags
- `develop`: Integration branch for ongoing development  
- `feature/*`: Individual feature development branches
- `milestone/*`: Milestone-specific feature branches

### Commit Message Convention
- type(scope): brief description
- feat(ingestion): add robust error handling to retail pipeline
- fix(schema): correct foreign key constraint in fact_sales
- docs(readme): update architecture documentation
- test(quality): add comprehensive data validation tests

## Development Workflow

This project follows GitFlow branching strategy:
- `main`: Production-ready code
- `develop`: Integration branch for features
- `feature/*`: Individual feature branches
- `milestone/*`: Milestone-specific branches

## Monitoring and Observability

### Logging
- **Application Logs**: `logs/ingestion.log`
- **Error Tracking**: Structured logging with correlation IDs
- **Performance Metrics**: Execution time and resource usage tracking

### Data Quality Monitoring
- Real-time quality score calculation
- Automated anomaly detection
- Business rule violation alerts
- Historical quality trend analysis

## Performance Considerations

### Database Optimization
- **Indexing Strategy**: Optimized for common query patterns
- **Partitioning**: Date-based partitioning for large fact tables
- **Constraints**: Business rule enforcement at database level
- **Statistics**: Regular ANALYZE for query optimization

### Pipeline Optimization  
- **Batch Processing**: Configurable batch sizes for memory management
- **Parallel Processing**: Multi-threaded ingestion for large datasets
- **Incremental Loading**: Change data capture for efficient updates
- **Resource Management**: Connection pooling and memory optimization

## Testing Strategy

### Data Quality Tests
- Schema validation and constraint testing
- Business rule compliance verification
- Data freshness and completeness checks
- Cross-layer consistency validation

### Pipeline Tests
- Unit tests for individual components
- Integration tests for end-to-end workflows
- Performance tests for scalability validation
- Error handling and recovery testing

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with proper tests and documentation
4. Commit with conventional commit messages
5. Push to your branch (`git push origin feature/amazing-feature`)
6. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- **UCI Machine Learning Repository** for providing the Online Retail dataset
- **Databricks** for pioneering the Medallion Architecture pattern
- **Modern data engineering community** for best practices and patterns

## Contact

**Gaurav Jain** - garv.jain.jpr@gmail.com
**Project Link**: https://github.com/yourusername/data-engineering-capstone

**Latest Update**: Milestone 2 completed with full Medallion Architecture implementation, comprehensive data quality framework, and production-ready ingestion pipeline.

