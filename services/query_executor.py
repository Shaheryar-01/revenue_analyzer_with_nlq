import logging
from typing import Dict, Any, List, Tuple
from services.supabase_client import SupabaseManager

logger = logging.getLogger(__name__)


class QueryExecutor:
    """
    Executes SQL queries with validation and safety checks.
    Works with Supabase database.
    """
    
    def __init__(self):
        """Initialize the query executor with Supabase connection"""
        self.supabase = SupabaseManager()
    
    def execute_sql(self, sql: str, upload_id: str) -> Dict[str, Any]:
        """
        Execute a SQL query with validation.
        
        Args:
            sql: SQL query to execute
            upload_id: Upload ID for data isolation
            
        Returns:
            dict: {
                'success': bool,
                'result': list of results,
                'row_count': number of rows returned,
                'error': error message if any
            }
        """
        
        logger.info(f"Executing SQL for upload_id: {upload_id}")
        logger.info(f"Query: {sql[:200]}...")
        
        # Validate query safety
        is_valid, validation_errors = self.validate_query_safety(sql)
        
        if not is_valid:
            error_msg = '; '.join(validation_errors)
            logger.error(f"Query validation failed: {error_msg}")
            return {
                'success': False,
                'result': [],
                'row_count': 0,
                'error': error_msg
            }
        
        # Execute query using Supabase
        try:
            result = self.supabase.execute_raw_sql(sql)
            
            if result.get('success'):
                data = result.get('data', [])
                row_count = len(data)
                
                # Log first result for debugging
                if data:
                    logger.info(f" First result: {data[0]}")
                
                # Post-execution validation
                warnings = self._validate_results(data)
                if warnings:
                    logger.warning(f"Result warnings: {'; '.join(warnings)}")
                
                logger.info(f"Query executed successfully. Rows returned: {row_count}")
                
                return {
                    'success': True,
                    'result': data,
                    'row_count': row_count,
                    'warnings': warnings
                }
            else:
                error = result.get('error', 'Unknown error')
                logger.error(f"Query execution failed: {error}")
                return {
                    'success': False,
                    'result': [],
                    'row_count': 0,
                    'error': error
                }
                
        except Exception as e:
            error_msg = f"Unexpected error executing query: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'result': [],
                'row_count': 0,
                'error': error_msg
            }
    
    def validate_query_safety(self, query: str) -> Tuple[bool, List[str]]:
        """
        Validate query for safety and correctness.
        
        Returns:
            Tuple of (is_valid, list of errors)
        """
        errors = []
        query_upper = query.upper()
        
        # Check for forbidden operations
        forbidden_operations = [
            'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE',
            'INSERT', 'UPDATE', 'GRANT', 'REVOKE', 'EXEC'
        ]
        
        for operation in forbidden_operations:
            if f' {operation} ' in f' {query_upper} ':
                errors.append(f"Forbidden operation: {operation}")
        
        # Check for forbidden column usage in aggregations
        forbidden_patterns = [
            ('SUM(TOTAL)', 'Cannot use SUM(total) - this inflates results by 12x'),
            ('SUM(YTD_ACTUAL)', 'Cannot use SUM(ytd_actual) - use SUM(actual)'),
            ('SUM(REMAINING_PROJECTION)', 'Cannot use SUM(remaining_projection)'),
            ('AVG(TOTAL)', 'Cannot use AVG(total) - use AVG(actual/projected/budget)'),
            ('MAX(TOTAL)', 'Cannot use MAX(total) - use MAX(actual/projected/budget)'),
            ('MIN(TOTAL)', 'Cannot use MIN(total) - use MIN(actual/projected/budget)'),
        ]
        
        query_normalized = query_upper.replace(' ', '').replace('\n', '')
        
        for pattern, error_msg in forbidden_patterns:
            pattern_normalized = pattern.replace(' ', '')
            if pattern_normalized in query_normalized:
                errors.append(error_msg)
        
        # Check if using aggregation with summary columns
        if 'SUM(' in query_upper or 'AVG(' in query_upper:
            summary_cols = ['TOTAL', 'YTD_ACTUAL', 'REMAINING_PROJECTION']
            for col in summary_cols:
                if col in query_upper:
                    import re
                    agg_pattern = rf'(SUM|AVG|MAX|MIN)\s*\(\s*{col}\s*\)'
                    if re.search(agg_pattern, query_upper):
                        errors.append(
                            f'Summary column {col} used in aggregation - '
                            'this will produce incorrect results'
                        )
        
        # Verify upload_id is present
        if 'upload_id' not in query.lower():
            errors.append('Query must filter by upload_id')
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    def _validate_results(self, data: List[Dict]) -> List[str]:
        """
        Validate query results for common issues.
        
        Returns:
            List of warning messages
        """
        warnings = []
        
        if not data:
            return warnings
        
        # Check for suspiciously large values
        for row in data:
            for key, value in row.items():
                if isinstance(value, (int, float)) and value > 500000:
                    warnings.append(
                        f"Warning: Large value ${value:,.2f} detected for '{key}'. "
                        "Please verify this is not using summary columns."
                    )
                    break
        
        return warnings
    
    def validate_result_quality(self, result, metadata, upload_id):
        """Validate result quality with smart numeric detection"""
        
        # Case 1: No results
        if not result or len(result) == 0:
            return self._handle_empty_result(metadata, upload_id)
        
        # Case 2: Single result - check for NULL or zero
        if len(result) == 1:
            first_result = result[0]
            
            # ✅ Find ANY numeric value in the result
            revenue_value = None
            for key, value in first_result.items():
                if isinstance(value, (int, float)) and value is not None:
                    revenue_value = value
                    break
            
            # Check for NULL
            if revenue_value is None:
                return {
                    'is_valid': True,
                    'has_warning': True,
                    'warning': 'Query matched records, but all revenue values are NULL or missing.'
                }
            
            # Check for zero
            if revenue_value == 0:
                filters = metadata.get('filters_applied', {})
                if filters:
                    return self._check_zero_result(filters, upload_id)
        
        # Case 3: Valid data
        return {
            'is_valid': True,
            'has_warning': False
        }




    def _handle_empty_result(self, metadata: Dict[str, Any], upload_id: str) -> Dict[str, Any]:
        """Handle case where query returns no results"""
        filters = metadata.get('filters_applied', {})
        
        if not filters:
            return {
                'is_valid': False,
                'message': 'No data found. The database may be empty.'
            }
        
        # Check each filter to see if it exists
        suggestions = []
        
        # Get entity metadata to check against
        entity_metadata = self.supabase.get_entity_metadata(upload_id)
        
        if entity_metadata:
            # Check Unit
            if 'region' in filters:
                region_value = filters['region']
                if region_value not in entity_metadata.get('regions', []):
                    # Check if it's actually a unit
                    if region_value in entity_metadata.get('units', []):
                        suggestions.append(f"'{region_value}' is a Unit, not a Region. Try: 'revenue for {region_value}'")
                    else:
                        available = ', '.join(entity_metadata.get('regions', [])[:5])
                        suggestions.append(f"Region '{region_value}' not found. Available regions: {available}")
            
            # Check Customer
            if 'customer' in filters:
                customer_value = filters['customer']
                # Check if any customer contains this substring
                matching_customers = [c for c in entity_metadata.get('customers', []) if customer_value.upper() in c.upper()]
                if not matching_customers:
                    available = ', '.join([c[:30] + '...' if len(c) > 30 else c for c in entity_metadata.get('customers', [])[:3]])
                    suggestions.append(f"Customer '{customer_value}' not found. Available customers: {available}")
                elif len(matching_customers) == 1:
                    suggestions.append(f"Using customer: {matching_customers[0]}")
            
            # Check Product
            if 'product' in filters:
                product_values = filters['product'] if isinstance(filters['product'], list) else [filters['product']]
                for product_value in product_values:
                    # Check if it's actually a unit
                    if product_value in entity_metadata.get('units', []):
                        suggestions.append(f"'{product_value}' is a Unit, not a Product. Try: 'revenue by unit'")
        
        if suggestions:
            message = "No data found matching your filters.\n\n" + "\n".join(f"• {s}" for s in suggestions)
        else:
            filter_desc = ', '.join([f"{k}: {v}" for k, v in filters.items()])
            message = f"No data found for filters: {filter_desc}"
        
        return {
            'is_valid': False,
            'message': message
        }
    
    def _check_zero_result(self, filters: Dict[str, Any], upload_id: str) -> Dict[str, Any]:
        """Check if $0 result is because no data exists or values are truly zero"""
        try:
            # Build a count query with same filters
            filter_conditions = []
            for key, value in filters.items():
                if key in ['region', 'unit', 'customer', 'category', 'product']:
                    if isinstance(value, list):
                        values_str = ','.join([f"'{v}'" for v in value])
                        filter_conditions.append(f"{key} IN ({values_str})")
                    else:
                        filter_conditions.append(f"{key} = '{value}'")
            
            where_clause = ' AND '.join(filter_conditions)
            count_sql = f"SELECT COUNT(*) as count FROM revenue_tracker WHERE upload_id = '{upload_id}'"
            if where_clause:
                count_sql += f" AND {where_clause}"
            
            count_result = self.supabase.execute_raw_sql(count_sql)
            
            if count_result.get('success') and count_result.get('data'):
                count = count_result['data'][0].get('count', 0)
                
                if count > 0:
                    filter_desc = ', '.join([f"{k}: {v}" for k, v in filters.items()])
                    return {
                        'is_valid': True,
                        'has_warning': True,
                        'warning': f'Found {count} records matching your filters ({filter_desc}), but total revenue is $0. All values may be zero or NULL.'
                    }
                else:
                    return {
                        'is_valid': False,
                        'message': 'No records found matching your filters.'
                    }
        
        except Exception as e:
            logger.error(f"Error checking zero result: {str(e)}")
        
        return {
            'is_valid': True,
            'has_warning': False
        }
    
    def test_query(self, sql: str, upload_id: str, limit: int = 5) -> Dict[str, Any]:
        """
        Test a query with a limit to preview results.
        
        Args:
            sql: SQL query to test
            upload_id: Upload ID for data isolation
            limit: Maximum rows to return
            
        Returns:
            Query results with limited rows
        """
        # Add LIMIT if not present
        if 'LIMIT' not in sql.upper():
            sql = sql.rstrip(';') + f' LIMIT {limit}'
        
        return self.execute_sql(sql, upload_id)