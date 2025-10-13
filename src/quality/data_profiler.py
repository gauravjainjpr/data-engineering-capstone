"""
Comprehensive data profiling system for the retail dataset.

This module provides detailed data quality assessment, profiling statistics,
and generates reports for understanding data characteristics and issues.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import json

from sqlalchemy import create_engine, text
from src.utils.database import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProfiler:
    """
    Comprehensive data profiling system for retail data quality assessment.
    
    This class analyzes data quality, generates statistics, and creates
    detailed reports for data understanding and quality monitoring.
    """
    
    def __init__(self, environment='development'):
        """Initialize the data profiler."""
        self.environment = environment
        self.db_manager = DatabaseManager(environment)
        self.engine = self.db_manager.get_engine()
        
    def profile_bronze_data(self) -> Dict[str, Any]:
        """
        Profile data in the Bronze layer and generate comprehensive statistics.
        
        Returns:
            Dict: Complete profiling results with statistics and quality metrics
        """
        logger.info("🔍 Starting comprehensive Bronze layer data profiling...")
        
        try:
            # Load data from Bronze layer
            query = """
            SELECT invoice, stock_code, description, quantity, invoice_date, 
                   unit_price, customer_id, country, load_timestamp, source_file
            FROM bronze.retail_raw 
            ORDER BY load_timestamp DESC
            LIMIT 600000  -- Profile recent data for performance
            """
            
            df = pd.read_sql(query, self.engine)
            logger.info(f"📊 Loaded {len(df):,} records for profiling")
            
            if df.empty:
                logger.warning("⚠️ No data found in Bronze layer")
                return {"error": "No data available for profiling"}
            
            # Generate comprehensive profile
            profile_results = {
                'profile_timestamp': datetime.now().isoformat(),
                'dataset_overview': self._generate_dataset_overview(df),
                'column_profiles': self._generate_column_profiles(df),
                'data_quality_assessment': self._assess_data_quality(df),
                'business_insights': self._generate_business_insights(df),
                'recommendations': self._generate_recommendations(df)
            }
            
            # Save profiling results
            self._save_profiling_results(profile_results)
            
            logger.info("✅ Data profiling completed successfully")
            return profile_results
            
        except Exception as e:
            logger.error(f"❌ Error during data profiling: {str(e)}")
            raise
    
    def _generate_dataset_overview(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate high-level dataset statistics."""
        logger.info("📋 Generating dataset overview...")
        
        # Convert invoice_date to datetime for analysis
        df['invoice_date'] = pd.to_datetime(df['invoice_date'])
        
        overview = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'date_range': {
                'min_date': df['invoice_date'].min().isoformat(),
                'max_date': df['invoice_date'].max().isoformat(),
                'date_span_days': (df['invoice_date'].max() - df['invoice_date'].min()).days
            },
            'unique_counts': {
                'invoices': df['invoice'].nunique(),
                'products': df['stock_code'].nunique(),
                'customers': df['customer_id'].nunique(),
                'countries': df['country'].nunique()
            },
            'data_freshness': {
                'latest_load': df['load_timestamp'].max().isoformat(),
                'oldest_load': df['load_timestamp'].min().isoformat()
            }
        }
        
        return overview
    
    def _generate_column_profiles(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """Generate detailed profiles for each column."""
        logger.info("🔬 Generating detailed column profiles...")
        
        column_profiles = {}
        
        for column in df.columns:
            if column in ['load_timestamp', 'source_file']:  # Skip audit columns
                continue
                
            logger.info(f"   Profiling column: {column}")
            
            profile = {
                'data_type': str(df[column].dtype),
                'non_null_count': df[column].count(),
                'null_count': df[column].isnull().sum(),
                'null_percentage': (df[column].isnull().sum() / len(df)) * 100,
                'unique_count': df[column].nunique(),
                'unique_percentage': (df[column].nunique() / len(df)) * 100 if len(df) > 0 else 0
            }
            
            # Add type-specific statistics
            if df[column].dtype in ['int64', 'float64']:
                profile.update(self._profile_numeric_column(df[column]))
            elif df[column].dtype == 'object':
                profile.update(self._profile_text_column(df[column]))
            elif pd.api.types.is_datetime64_any_dtype(df[column]):
                profile.update(self._profile_datetime_column(df[column]))
            
            column_profiles[column] = profile
        
        return column_profiles
    
    def _profile_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile numeric columns with statistical measures."""
        numeric_profile = {
            'min_value': float(series.min()) if not series.empty else None,
            'max_value': float(series.max()) if not series.empty else None,
            'mean': float(series.mean()) if not series.empty else None,
            'median': float(series.median()) if not series.empty else None,
            'std_dev': float(series.std()) if not series.empty else None,
            'quartiles': {
                'q1': float(series.quantile(0.25)) if not series.empty else None,
                'q3': float(series.quantile(0.75)) if not series.empty else None
            },
            'zero_count': (series == 0).sum(),
            'negative_count': (series < 0).sum(),
            'positive_count': (series > 0).sum()
        }
        
        # Detect outliers using IQR method
        if not series.empty and series.nunique() > 1:
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = ((series < lower_bound) | (series > upper_bound)).sum()
            numeric_profile['outlier_count'] = outliers
            numeric_profile['outlier_percentage'] = (outliers / len(series)) * 100
        
        return numeric_profile
    
    def _profile_text_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile text columns with string statistics."""
        text_profile = {
            'avg_length': series.str.len().mean() if not series.empty else None,
            'min_length': series.str.len().min() if not series.empty else None,
            'max_length': series.str.len().max() if not series.empty else None,
            'empty_string_count': (series == '').sum(),
            'whitespace_only_count': series.str.strip().eq('').sum(),
            'top_values': series.value_counts().head(10).to_dict()
        }
        
        # Check for potential data quality issues
        if not series.empty:
            text_profile['has_leading_trailing_spaces'] = series.str.strip().ne(series).sum()
            text_profile['has_special_characters'] = series.str.contains(r'[^\w\s-]', na=False).sum()
        
        return text_profile
    
    def _profile_datetime_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile datetime columns with temporal statistics."""
        datetime_profile = {
            'min_date': series.min().isoformat() if not series.empty else None,
            'max_date': series.max().isoformat() if not series.empty else None,
            'date_range_days': (series.max() - series.min()).days if not series.empty else None
        }
        
        if not series.empty:
            # Extract temporal components
            datetime_profile.update({
                'unique_years': series.dt.year.nunique(),
                'unique_months': series.dt.month.nunique(),
                'unique_days': series.dt.day.nunique(),
                'unique_hours': series.dt.hour.nunique(),
                'weekend_transactions': series.dt.weekday.isin([5, 6]).sum(),
                'business_hours_transactions': series.dt.hour.between(9, 17).sum()
            })
        
        return datetime_profile
    
    def _assess_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess overall data quality with business rules."""
        logger.info("🎯 Assessing data quality...")
        
        quality_assessment = {
            'overall_score': 0,
            'completeness_score': 0,
            'validity_score': 0,
            'consistency_score': 0,
            'issues': [],
            'severity_counts': {'critical': 0, 'high': 0, 'medium': 0, 'low': 0}
        }
        
        # Completeness Assessment
        total_cells = len(df) * len(df.columns)
        null_cells = df.isnull().sum().sum()
        completeness_score = ((total_cells - null_cells) / total_cells) * 100
        quality_assessment['completeness_score'] = completeness_score
        
        # Critical completeness issues
        critical_columns = ['invoice', 'stock_code', 'quantity', 'unit_price', 'country']
        for col in critical_columns:
            if col in df.columns:
                null_pct = (df[col].isnull().sum() / len(df)) * 100
                if null_pct > 5:  # More than 5% nulls in critical columns
                    quality_assessment['issues'].append({
                        'type': 'completeness',
                        'severity': 'critical',
                        'column': col,
                        'description': f"High null percentage in critical column: {null_pct:.1f}%"
                    })
                    quality_assessment['severity_counts']['critical'] += 1
        
        # Validity Assessment
        validity_issues = 0
        total_validity_checks = 0
        
        # Check price validity
        if 'unit_price' in df.columns:
            invalid_prices = (df['unit_price'] < 0).sum()
            total_validity_checks += len(df)
            validity_issues += invalid_prices
            
            if invalid_prices > 0:
                quality_assessment['issues'].append({
                    'type': 'validity',
                    'severity': 'high',
                    'column': 'unit_price',
                    'description': f"Found {invalid_prices} records with negative prices"
                })
                quality_assessment['severity_counts']['high'] += 1
        
        # Check quantity validity
        if 'quantity' in df.columns:
            zero_quantities = (df['quantity'] == 0).sum()
            total_validity_checks += len(df)
            validity_issues += zero_quantities
            
            if zero_quantities > 0:
                quality_assessment['issues'].append({
                    'type': 'validity',
                    'severity': 'medium',
                    'column': 'quantity',
                    'description': f"Found {zero_quantities} records with zero quantity"
                })
                quality_assessment['severity_counts']['medium'] += 1
        
        validity_score = ((total_validity_checks - validity_issues) / total_validity_checks) * 100 if total_validity_checks > 0 else 100
        quality_assessment['validity_score'] = validity_score
        
        # Consistency Assessment (basic checks)
        consistency_score = 100  # Start with perfect score
        
        # Check for duplicate records
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            consistency_score -= 10
            quality_assessment['issues'].append({
                'type': 'consistency',
                'severity': 'medium',
                'description': f"Found {duplicates} duplicate records"
            })
            quality_assessment['severity_counts']['medium'] += 1
        
        quality_assessment['consistency_score'] = consistency_score
        
        # Calculate overall score
        overall_score = (completeness_score + validity_score + consistency_score) / 3
        quality_assessment['overall_score'] = overall_score
        
        return quality_assessment
    
    def _generate_business_insights(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate business-relevant insights from the data."""
        logger.info("💼 Generating business insights...")
        
        insights = {}
        
        try:
            # Revenue analysis
            if all(col in df.columns for col in ['quantity', 'unit_price']):
                df['revenue'] = df['quantity'] * df['unit_price']
                insights['revenue_analysis'] = {
                    'total_revenue': float(df['revenue'].sum()),
                    'average_transaction_value': float(df['revenue'].mean()),
                    'median_transaction_value': float(df['revenue'].median()),
                    'highest_single_transaction': float(df['revenue'].max()),
                    'negative_revenue_transactions': (df['revenue'] < 0).sum()
                }
            
            # Customer analysis
            if 'customer_id' in df.columns:
                customer_data = df[df['customer_id'].notna()]
                insights['customer_analysis'] = {
                    'total_customers': customer_data['customer_id'].nunique(),
                    'transactions_with_customer_id': len(customer_data),
                    'anonymous_transactions': (df['customer_id'].isna()).sum(),
                    'customer_id_coverage': (len(customer_data) / len(df)) * 100
                }
                
                if len(customer_data) > 0:
                    customer_transaction_counts = customer_data['customer_id'].value_counts()
                    insights['customer_analysis'].update({
                        'avg_transactions_per_customer': float(customer_transaction_counts.mean()),
                        'max_transactions_by_single_customer': customer_transaction_counts.max(),
                        'repeat_customers': (customer_transaction_counts > 1).sum()
                    })
            
            # Geographic analysis
            if 'country' in df.columns:
                country_analysis = df['country'].value_counts()
                insights['geographic_analysis'] = {
                    'total_countries': df['country'].nunique(),
                    'top_countries': country_analysis.head(10).to_dict(),
                    'uk_transactions': (df['country'] == 'United Kingdom').sum(),
                    'international_transactions': (df['country'] != 'United Kingdom').sum()
                }
            
            # Product analysis
            if 'stock_code' in df.columns:
                product_analysis = df['stock_code'].value_counts()
                insights['product_analysis'] = {
                    'total_products': df['stock_code'].nunique(),
                    'most_popular_products': product_analysis.head(10).to_dict(),
                    'single_transaction_products': (product_analysis == 1).sum()
                }
            
            # Temporal analysis
            if 'invoice_date' in df.columns:
                df['invoice_date'] = pd.to_datetime(df['invoice_date'])
                df['hour'] = df['invoice_date'].dt.hour
                df['day_of_week'] = df['invoice_date'].dt.day_name()
                df['month'] = df['invoice_date'].dt.month_name()
                
                insights['temporal_analysis'] = {
                    'peak_hours': df['hour'].value_counts().head(5).to_dict(),
                    'busiest_days': df['day_of_week'].value_counts().to_dict(),
                    'seasonal_patterns': df['month'].value_counts().to_dict(),
                    'weekend_vs_weekday': {
                        'weekend_transactions': df['day_of_week'].isin(['Saturday', 'Sunday']).sum(),
                        'weekday_transactions': (~df['day_of_week'].isin(['Saturday', 'Sunday'])).sum()
                    }
                }
        
        except Exception as e:
            logger.error(f"❌ Error generating business insights: {str(e)}")
            insights['error'] = f"Failed to generate insights: {str(e)}"
        
        return insights
    
    def _generate_recommendations(self, df: pd.DataFrame) -> List[Dict[str, str]]:
        """Generate actionable recommendations based on profiling results."""
        logger.info("💡 Generating recommendations...")
        
        recommendations = []
        
        # Check for high null rates
        null_percentages = df.isnull().mean() * 100
        high_null_columns = null_percentages[null_percentages > 10].index.tolist()
        
        if high_null_columns:
            recommendations.append({
                'category': 'Data Quality',
                'priority': 'High',
                'issue': f"High null rates in columns: {', '.join(high_null_columns)}",
                'recommendation': 'Investigate data sources and implement validation at ingestion point'
            })
        
        # Check for negative prices
        if 'unit_price' in df.columns and (df['unit_price'] < 0).any():
            recommendations.append({
                'category': 'Business Rules',
                'priority': 'Critical',
                'issue': 'Negative unit prices detected',
                'recommendation': 'Implement price validation constraints and investigate source system'
            })
        
        # Check for zero quantities
        if 'quantity' in df.columns and (df['quantity'] == 0).any():
            recommendations.append({
                'category': 'Data Validation',
                'priority': 'Medium',
                'issue': 'Zero quantity transactions found',
                'recommendation': 'Clarify business rules for zero-quantity transactions or filter them out'
            })
        
        # Check customer ID coverage
        if 'customer_id' in df.columns:
            missing_customer_pct = (df['customer_id'].isna().sum() / len(df)) * 100
            if missing_customer_pct > 20:
                recommendations.append({
                    'category': 'Customer Analytics',
                    'priority': 'Medium',
                    'issue': f'{missing_customer_pct:.1f}% of transactions lack customer IDs',
                    'recommendation': 'Improve customer identification processes for better analytics'
                })
        
        # Performance recommendations
        if len(df) > 500000:
            recommendations.append({
                'category': 'Performance',
                'priority': 'Medium',
                'issue': 'Large dataset detected',
                'recommendation': 'Consider implementing data partitioning and archiving strategies'
            })
        
        return recommendations
    
    def _save_profiling_results(self, results: Dict[str, Any]):
        """Save profiling results to file and database."""
        try:
            # Save to JSON file
            output_dir = Path('data/staging/profiling')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = output_dir / f'data_profile_{timestamp}.json'
            
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"📄 Profiling results saved to: {output_file}")
            
            # Save summary to database
            self._save_profile_summary_to_db(results)
            
        except Exception as e:
            logger.error(f"❌ Error saving profiling results: {str(e)}")
    
    def _save_profile_summary_to_db(self, results: Dict[str, Any]):
        """Save profile summary to the Silver layer for tracking."""
        try:
            summary_metrics = [
                ('total_records', results['dataset_overview']['total_records']),
                ('overall_quality_score', results['data_quality_assessment']['overall_score']),
                ('completeness_score', results['data_quality_assessment']['completeness_score']),
                ('validity_score', results['data_quality_assessment']['validity_score']),
                ('consistency_score', results['data_quality_assessment']['consistency_score']),
                ('critical_issues', results['data_quality_assessment']['severity_counts']['critical']),
                ('high_issues', results['data_quality_assessment']['severity_counts']['high']),
                ('medium_issues', results['data_quality_assessment']['severity_counts']['medium'])
            ]
            
            with self.engine.connect() as conn:
                for metric_name, metric_value in summary_metrics:
                    conn.execute(text("""
                        INSERT INTO silver.data_quality_metrics 
                        (load_id, table_name, metric_name, metric_value, metric_description)
                        SELECT load_id, 'bronze.retail_raw', :metric_name, :metric_value, 
                               'Data profiling metric from automated assessment'
                        FROM bronze.load_metadata 
                        WHERE load_status = 'COMPLETED'
                        ORDER BY load_start_time DESC 
                        LIMIT 1
                    """), {
                        'metric_name': metric_name,
                        'metric_value': float(metric_value) if isinstance(metric_value, (int, float)) else 0
                    })
                
                conn.commit()
                logger.info("✅ Profile summary saved to database")
                
        except Exception as e:
            logger.error(f"❌ Error saving profile summary to database: {str(e)}")

def main():
    """Main function to run data profiling."""
    profiler = DataProfiler()
    
    try:
        results = profiler.profile_bronze_data()
        
        # Print summary
        print("\n" + "="*60)
        print("📊 DATA PROFILING SUMMARY")
        print("="*60)
        
        if 'error' in results:
            print(f"❌ Error: {results['error']}")
            return
        
        overview = results['dataset_overview']
        quality = results['data_quality_assessment']
        
        print(f"📋 Dataset Overview:")
        print(f"   Total Records: {overview['total_records']:,}")
        print(f"   Date Range: {overview['date_range']['min_date']} to {overview['date_range']['max_date']}")
        print(f"   Countries: {overview['unique_counts']['countries']}")
        print(f"   Products: {overview['unique_counts']['products']:,}")
        print(f"   Customers: {overview['unique_counts']['customers']:,}")
        
        print(f"\n🎯 Data Quality Scores:")
        print(f"   Overall Score: {quality['overall_score']:.1f}%")
        print(f"   Completeness: {quality['completeness_score']:.1f}%")
        print(f"   Validity: {quality['validity_score']:.1f}%")
        print(f"   Consistency: {quality['consistency_score']:.1f}%")
        
        print(f"\n⚠️  Issues Summary:")
        severity_counts = quality['severity_counts']
        print(f"   Critical: {severity_counts['critical']}")
        print(f"   High: {severity_counts['high']}")
        print(f"   Medium: {severity_counts['medium']}")
        print(f"   Low: {severity_counts['low']}")
        
        if 'revenue_analysis' in results['business_insights']:
            revenue = results['business_insights']['revenue_analysis']
            print(f"\n💰 Business Insights:")
            print(f"   Total Revenue: £{revenue['total_revenue']:,.2f}")
            print(f"   Avg Transaction: £{revenue['average_transaction_value']:.2f}")
        
        print(f"\n💡 Recommendations: {len(results['recommendations'])} items")
        for i, rec in enumerate(results['recommendations'][:3], 1):
            print(f"   {i}. [{rec['priority']}] {rec['issue']}")
        
        print("="*60)
        
    except Exception as e:
        logger.error(f"💥 Profiling failed: {str(e)}")

if __name__ == "__main__":
    main()