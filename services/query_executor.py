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