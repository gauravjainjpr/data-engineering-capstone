# Data Engineering Capstone Project

A comprehensive end-to-end data engineering pipeline using the UCI Online Retail dataset.

## Project Overview

This project demonstrates modern data engineering practices by building a complete pipeline that:
- Ingests data from the UCI Online Retail dataset
- Performs data cleaning and transformation using dbt
- Orchestrates workflows with Apache Airflow
- Ensures data quality with Great Expectations
- Processes streaming data with Apache Kafka
- Handles big data with Apache Spark
- Visualizes insights with Tableau/Power BI

## Dataset

**Source**: UCI Machine Learning Repository - Online Retail II Dataset
**Records**: 1,067,371 transactions
**Time Period**: December 2009 to December 2011
**Description**: Transactions from a UK-based online retail company specializing in unique all-occasion gifts.

## Project Structure

data-engineering-capstone/
├── data/
│ ├── raw/ # Raw, unprocessed data
│ ├── staging/ # Intermediate processed data
│ └── warehouse/ # Final, analysis-ready data
├── notebooks/
│ ├── exploration/ # Data exploration notebooks
│ ├── transformation/# Data transformation notebooks
│ └── analysis/ # Data analysis notebooks
├── src/
│ ├── ingestion/ # Data ingestion scripts
│ ├── transformation/# Data transformation logic
│ └── quality/ # Data quality checks
├── config/ # Configuration files
├── logs/ # Application logs
├── docs/ # Project documentation
├── tests/ # Unit and integration tests
└── scripts/ # Utility scripts


## Getting Started

### Prerequisites
- Python 3.9+
- PostgreSQL 13+
- Git

### Setup
1. Clone the repository
2. Create virtual environment: `python -m venv venv`
3. Activate virtual environment: `source venv/bin/activate` (Linux/macOS) or `venv\Scripts\activate` (Windows)
4. Install dependencies: `pip install -r requirements.txt`
5. Configure database connection in `config/database.yml`

## Development Workflow

This project follows GitFlow branching strategy:
- `main`: Production-ready code
- `develop`: Integration branch for features
- `feature/*`: Individual feature branches
- `milestone/*`: Milestone-specific branches

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

[Gaurav Jain] - [garv.jain.jpr@gmail.com]

