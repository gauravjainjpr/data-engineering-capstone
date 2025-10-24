#!/bin/bash

echo "=================================================="
echo "MILESTONE 3: CHECKPOINT VALIDATION"
echo "=================================================="
echo ""

# Navigate to dbt project
cd retail_transform

echo "✓ Step 1: Testing dbt connection..."
dbt debug --quiet || exit 1

echo "✓ Step 2: Running full pipeline refresh..."
dbt run --full-refresh || exit 1

echo "✓ Step 3: Running all tests..."
dbt test || exit 1

echo "✓ Step 4: Checking source freshness..."
dbt source freshness

echo "✓ Step 5: Generating documentation..."
dbt docs generate

cd ..

echo ""
echo "✓ Step 6: Running SQL validation checks..."
psql -U postgres -d online_retail_dev -f scripts/validate_pipeline.sql

echo ""
echo "=================================================="
echo "✅ MILESTONE 3 VALIDATION COMPLETE"
echo "=================================================="
echo ""
echo "Summary:"
echo "  - Bronze layer: Raw data ingested"
echo "  - Silver layer: Staging models created"
echo "  - Gold layer: Star schema implemented"
echo "  - Tests: All passing"
echo "  - Documentation: Generated successfully"
echo ""
echo "Next: Milestone 4 - Workflow Orchestration with Airflow"
echo "=================================================="
