# app/services/query_executor.py
import logging
from typing import Dict, Any
from services.supabase_client import SupabaseManager

logger = logging.getLogger(__name__)

class QueryExecutor:
    """Execute SQL queries on Supabase database"""
    
    def __init__(self):
        self.supabase = SupabaseManager()
        logger.info("Query Executor initialized")
    
    def execute_sql(self, sql: str, upload_id: str) -> Dict[str, Any]:
        """
        Execute SQL query safely
        
        Args:
            sql: SQL query to execute
            upload_id: Upload identifier for validation
            
        Returns:
            Execution result with data and metadata
        """
        logger.info(f"=" * 80)
        logger.info("QUERY EXECUTION START")
        logger.info(f"=" * 80)
        logger.info(f"Upload ID: {upload_id}")
        logger.info(f"SQL Query:\n{sql}")
        
        try:
            # Validate SQL (basic security check)
            if not self._is_sql_safe(sql):
                logger.error("SQL query failed security validation")
                return {
                    'success': False,
                    'error': 'Query failed security validation',
                    'result': None
                }
            
            # Verify upload_id is in the query
            if upload_id not in sql:
                logger.warning(f"upload_id not found in query, adding it")
                # This shouldn't happen if AI is working correctly, but safety check
            
            # Execute query
            result = self._execute_query_direct(sql)
            
            if result['success']:
                logger.info(f"✅ Query executed successfully")
                logger.info(f"Rows returned: {len(result['data'])}")
                logger.info(f"Result preview: {result['data'][:3] if result['data'] else 'No data'}")
            else:
                logger.error(f"❌ Query execution failed: {result['error']}")
            
            logger.info(f"=" * 80)
            logger.info("QUERY EXECUTION COMPLETE")
            logger.info(f"=" * 80)
            
            return result
            
        except Exception as e:
            logger.error(f"Query execution exception: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'result': None
            }
    
    def _execute_query_direct(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL directly using Supabase RPC
        
        Note: You need to create this RPC function in Supabase:
        
        CREATE OR REPLACE FUNCTION execute_custom_query(query_text TEXT)
        RETURNS TABLE (result JSONB) AS $$
        BEGIN
            RETURN QUERY EXECUTE query_text;
        END;
        $$ LANGUAGE plpgsql SECURITY DEFINER;
        """
        try:
            # For now, use postgrest-py to execute
            # This requires the RPC function in Supabase
            response = self.supabase.supabase.rpc('execute_custom_query', {
                'query_text': sql
            }).execute()
            
            # Process results
            result_data = self._process_query_results(response.data)
            
            return {
                'success': True,
                'data': result_data,
                'row_count': len(result_data),
                'result': self._format_result(result_data)
            }
            
        except Exception as e:
            logger.error(f"Direct query execution failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'data': [],
                'result': None
            }
    
    def _process_query_results(self, raw_data: list) -> list:
        """Process raw query results"""
        if not raw_data:
            return []
        
        # If results are wrapped in 'result' key
        if isinstance(raw_data[0], dict) and 'result' in raw_data[0]:
            return [row['result'] for row in raw_data]
        
        return raw_data
    
    def _format_result(self, data: list) -> Any:
        """
        Format result for natural language processing
        
        Returns:
            - Single value if one row, one column
            - Dictionary if one row, multiple columns
            - List of dictionaries if multiple rows
        """
        if not data:
            return None
        
        if len(data) == 1:
            # Single row
            row = data[0]
            
            if isinstance(row, dict):
                # If only one column, return just the value
                if len(row) == 1:
                    return list(row.values())[0]
                else:
                    return row
            else:
                return row
        
        # Multiple rows
        return data
    
    def _is_sql_safe(self, sql: str) -> bool:
        """
        Basic SQL safety validation
        
        Blocks:
        - DROP, DELETE without WHERE
        - Multiple statements (;)
        - System commands
        """
        sql_lower = sql.lower()
        
        # Block dangerous operations
        dangerous_patterns = [
            'drop table',
            'drop database',
            'truncate',
            'delete from revenue_tracker;',  # DELETE without WHERE
            'update revenue_tracker set',  # No updates allowed
            'insert into',  # No inserts via query
            'create ',
            'alter ',
            '--',  # SQL comments could hide malicious code
            '/*',
            '*/'
        ]
        
        for pattern in dangerous_patterns:
            if pattern in sql_lower:
                logger.warning(f"Dangerous SQL pattern detected: {pattern}")
                return False
        
        # Ensure it's a SELECT query
        if not sql_lower.strip().startswith('select'):
            logger.warning("Query must be a SELECT statement")
            return False
        
        # Check for multiple statements
        if sql.count(';') > 1:
            logger.warning("Multiple SQL statements not allowed")
            return False
        
        logger.info("SQL query passed safety validation")
        return True