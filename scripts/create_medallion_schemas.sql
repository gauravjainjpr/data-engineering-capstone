-- ============================================================================
-- Medallion Architecture Schema Creation
-- ============================================================================
-- This script creates the three-layer medallion architecture:
-- 1. bronze: Raw data ingestion with metadata
-- 2. silver: Cleaned and standardized data
-- 3. gold: Business-ready analytical models (star schema)
--
-- Each schema represents a data quality tier, enabling progressive
-- refinement and clear separation of concerns.
-- ============================================================================

-- Drop existing schemas if they exist (for development/testing only)
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;

-- ============================================================================
-- BRONZE LAYER: Raw Data Landing Zone
-- ============================================================================
-- Purpose: Store raw data exactly as received from sources
-- Characteristics:
--   - Minimal transformation
--   - Includes metadata for lineage and auditing
--   - Immutable (append-only, no updates/deletes)
--   - Preserves source data format and structure
-- ============================================================================

CREATE SCHEMA bronze;

COMMENT ON SCHEMA bronze IS 'Bronze Layer: Raw data ingestion zone with lineage metadata. Data is stored as-is from source systems with minimal transformation.';

-- ============================================================================
-- SILVER LAYER: Cleaned and Conformed Data
-- ============================================================================
-- Purpose: Store validated, cleaned, and standardized data
-- Characteristics:
--   - Data quality rules applied
--   - Duplicates removed
--   - Data types corrected
--   - Business rules enforced
--   - Ready for analysis and modeling
-- ============================================================================

CREATE SCHEMA silver;

COMMENT ON SCHEMA silver IS 'Silver Layer: Cleaned and validated data zone. Data is deduplicated, standardized, and conformed to business rules.';

-- ============================================================================
-- GOLD LAYER: Business-Ready Analytical Models
-- ============================================================================
-- Purpose: Store aggregated, business-ready datasets in star schema
-- Characteristics:
--   - Dimensional modeling (facts and dimensions)
--   - Optimized for query performance
--   - Pre-aggregated metrics
--   - Business-friendly naming and structure
--   - Denormalized for analytics
-- ============================================================================

CREATE SCHEMA gold;

COMMENT ON SCHEMA gold IS 'Gold Layer: Business-ready analytical zone. Data is modeled using star schema with fact and dimension tables optimized for BI and analytics.';

-- ============================================================================
-- Grant Permissions (Basic Setup)
-- ============================================================================
-- In production, implement more granular role-based access control
-- ============================================================================

-- Grant usage on schemas to de_user
GRANT USAGE ON SCHEMA bronze TO de_user;
GRANT USAGE ON SCHEMA silver TO de_user;
GRANT USAGE ON SCHEMA gold TO de_user;

-- Grant all privileges on all tables in schemas (for development)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO de_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO de_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO de_user;

-- Grant privileges on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO de_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO de_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO de_user;

-- Grant sequence privileges for auto-increment columns
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO de_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA silver TO de_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold TO de_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON SEQUENCES TO de_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON SEQUENCES TO de_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON SEQUENCES TO de_user;

-- Verify schemas were created
SELECT schema_name, 
       pg_catalog.obj_description(oid) as description
FROM information_schema.schemata s
JOIN pg_namespace n ON s.schema_name = n.nspname
WHERE schema_name IN ('bronze', 'silver', 'gold')
ORDER BY schema_name;