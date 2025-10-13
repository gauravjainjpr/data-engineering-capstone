"""
Verification script to validate successful data ingestion.

This script performs comprehensive checks on the ingested data
to ensure the pipeline worked correctly and data quality is acceptable.
"""

import logging
from sqlalchemy import create_engine, text
from src.utils.database import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_medallion_architecture():
    """Verify that all Medallion Architecture components are working."""
    logger.info("🔍 Verifying Medallion Architecture...")
    
    db_manager = DatabaseManager()
    engine = db_manager.get_engine()
    
    verification_results = {
        'schemas_exist': False,
        'bronze_data_loaded': False,
        'data_quality_acceptable': False,
        'load_metadata_complete': False
    }
    
    try:
        with engine.connect() as conn:
            # Check schemas exist
            schema_check = conn.execute(text("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN ('bronze', 'silver', 'gold')
            """)).fetchall()
            
            schema_names = [row[0] for row in schema_check]
            if len(schema_names) == 3:
                verification_results['schemas_exist'] = True
                logger.info("✅ All schemas (bronze, silver, gold) exist")
            else:
                logger.error(f"❌ Missing schemas. Found: {schema_names}")
            
            # Check Bronze data
            bronze_count = conn.execute(text("""
                SELECT COUNT(*) FROM bronze.retail_raw
            """)).scalar()
            
            if bronze_count > 0:
                verification_results['bronze_data_loaded'] = True
                logger.info(f"✅ Bronze layer contains {bronze_count:,} records")
            else:
                logger.error("❌ No data found in Bronze layer")
            
            # Check load metadata
            metadata_check = conn.execute(text("""
                SELECT load_status, COUNT(*) as count
                FROM bronze.load_metadata 
                GROUP BY load_status
            """)).fetchall()
            
            metadata_status = {row[0]: row[1] for row in metadata_check}
            if 'COMPLETED' in metadata_status or 'COMPLETED_WITH_ERRORS' in metadata_status:
                verification_results['load_metadata_complete'] = True
                logger.info(f"✅ Load metadata tracked: {metadata_status}")
            else:
                logger.error(f"❌ No successful loads found: {metadata_status}")
            
            # Basic data quality check
            quality_check = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(invoice) as invoices_with_data,
                    COUNT(customer_id) as records_with_customer,
                    AVG(CASE WHEN unit_price > 0 THEN 1.0 ELSE 0.0 END) as valid_price_ratio,
                    AVG(CASE WHEN quantity != 0 THEN 1.0 ELSE 0.0 END) as non_zero_quantity_ratio
                FROM bronze.retail_raw
            """)).fetchone()
            
            if quality_check:
                total, invoices, customers, price_ratio, qty_ratio = quality_check
                
                logger.info(f"📊 Data Quality Check:")
                logger.info(f"   Total records: {total:,}")
                logger.info(f"   Records with invoices: {invoices:,}")
                logger.info(f"   Records with customer ID: {customers:,}")
                logger.info(f"   Valid price ratio: {price_ratio:.2%}")
                logger.info(f"   Non-zero quantity ratio: {qty_ratio:.2%}")
                
                # Consider quality acceptable if most basic checks pass
                if price_ratio > 0.95 and qty_ratio > 0.95:
                    verification_results['data_quality_acceptable'] = True
                    logger.info("✅ Data quality is acceptable")
                else:
                    logger.warning("⚠️ Data quality concerns detected")
            
    except Exception as e:
        logger.error(f"❌ Verification failed: {str(e)}")
        return verification_results
    
    # Print summary
    print("\n" + "="*50)
    print("🏁 VERIFICATION SUMMARY")
    print("="*50)
    
    all_checks_passed = all(verification_results.values())
    
    for check, passed in verification_results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{check.replace('_', ' ').title()}: {status}")
    
    if all_checks_passed:
        print("\n🎉 All verification checks PASSED!")
        print("✅ Medallion Architecture is ready for next milestone")
    else:
        print("\n⚠️ Some verification checks FAILED!")
        print("❌ Please review and fix issues before proceeding")
    
    print("="*50)
    
    return verification_results

if __name__ == "__main__":
    verify_medallion_architecture()