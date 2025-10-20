# app/services/supabase_client.py
from supabase import create_client, Client
from config.settings import get_settings
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class SupabaseManager:
    """Manages Supabase database operations"""
    
    def __init__(self):
        settings = get_settings()
        self.supabase: Client = create_client(
            settings.supabase_url,
            settings.supabase_key
        )
        logger.info("Supabase client initialized successfully")
    
    def insert_revenue_data(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Insert revenue data into Supabase
        
        Args:
            data: List of dictionaries containing revenue records
            
        Returns:
            Response from Supabase
        """
        try:
            logger.info(f"Inserting {len(data)} records into revenue_tracker")
            
            response = self.supabase.table('revenue_tracker').insert(data).execute()
            
            logger.info(f"Successfully inserted {len(data)} records")
            return {
                'success': True,
                'rows_inserted': len(data),
                'message': f'Inserted {len(data)} records successfully'
            }
            
        except Exception as e:
            logger.error(f"Failed to insert data: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def save_upload_metadata(self, upload_id: str, filename: str, total_rows: int) -> bool:
        """Save upload metadata"""
        try:
            self.supabase.table('upload_metadata').insert({
                'upload_id': upload_id,
                'filename': filename,
                'total_rows': total_rows,
                'status': 'completed'
            }).execute()
            
            logger.info(f"Upload metadata saved for {upload_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save upload metadata: {str(e)}")
            return False
    
    def execute_raw_sql(self, sql: str) -> Dict[str, Any]:
        """
        Execute raw SQL query using Supabase RPC function.
        
        Note: You need to create this RPC function in Supabase:
        
        CREATE OR REPLACE FUNCTION execute_custom_query(query_text TEXT)
        RETURNS TABLE(result JSONB) AS $$
        BEGIN
            RETURN QUERY EXECUTE query_text;
        END;
        $$ LANGUAGE plpgsql SECURITY DEFINER;
        
        Args:
            sql: SQL query to execute
            
        Returns:
            dict: {
                'success': bool,
                'data': list of results,
                'error': error message if any
            }
        """
        try:
            logger.info(f"Executing raw SQL: {sql[:200]}...")
            
            # Execute using rpc function
            response = self.supabase.rpc('execute_custom_query', {
                'query_text': sql
            }).execute()
            
            logger.info(f"Query executed successfully, returned {len(response.data)} rows")
            
            return {
                'success': True,
                'data': response.data
            }
            
        except Exception as e:
            logger.error(f"Raw SQL execution failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'data': [],
                'error': str(e)
            }
    
    def execute_query(self, upload_id: str, sql: str) -> Dict[str, Any]:
        """
        Execute a SQL query using Supabase PostgREST
        
        Note: For complex SQL, we'll use the RPC function or direct SQL execution
        """
        try:
            logger.info(f"Executing query for upload_id: {upload_id}")
            logger.info(f"SQL: {sql}")
            
            # Execute using rpc function
            response = self.supabase.rpc('execute_custom_query', {
                'query_text': sql
            }).execute()
            
            logger.info(f"Query executed successfully, returned {len(response.data)} rows")
            
            return {
                'success': True,
                'data': response.data,
                'row_count': len(response.data)
            }
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'data': []
            }
    
    def execute_simple_query(self, upload_id: str, filters: Dict[str, Any], 
                           aggregation: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute simple queries using Supabase SDK (faster than raw SQL)
        
        Args:
            upload_id: Upload identifier
            filters: Dictionary of column filters
            aggregation: Optional aggregation function (sum, avg, count, etc.)
        """
        try:
            query = self.supabase.table('revenue_tracker').select('*')
            
            # Always filter by upload_id
            query = query.eq('upload_id', upload_id)
            
            # Apply additional filters
            for column, value in filters.items():
                if isinstance(value, list):
                    query = query.in_(column, value)
                else:
                    query = query.eq(column, value)
            
            response = query.execute()
            
            return {
                'success': True,
                'data': response.data,
                'row_count': len(response.data)
            }
            
        except Exception as e:
            logger.error(f"Simple query failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'data': []
            }
    
    def get_upload_info(self, upload_id: str) -> Optional[Dict[str, Any]]:
        """Get upload metadata"""
        try:
            response = self.supabase.table('upload_metadata').select('*').eq(
                'upload_id', upload_id
            ).execute()
            
            if response.data and len(response.data) > 0:
                return response.data[0]
            return None
            
        except Exception as e:
            logger.error(f"Failed to get upload info: {str(e)}")
            return None
    
    def delete_upload(self, upload_id: str) -> bool:
        """Delete all data for a specific upload"""
        try:
            logger.info(f"Deleting all data for upload_id: {upload_id}")
            
            # Delete from revenue_tracker
            self.supabase.table('revenue_tracker').delete().eq(
                'upload_id', upload_id
            ).execute()
            
            # Delete from upload_metadata
            self.supabase.table('upload_metadata').delete().eq(
                'upload_id', upload_id
            ).execute()
            
            logger.info(f"Successfully deleted data for upload_id: {upload_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete upload data: {str(e)}", exc_info=True)
            return False
    
    def get_distinct_values(self, upload_id: str, column: str) -> List[str]:
        """Get distinct values for a column"""
        try:
            response = self.supabase.table('revenue_tracker').select(column).eq(
                'upload_id', upload_id
            ).execute()
            
            values = set([row[column] for row in response.data if row.get(column)])
            return sorted(list(values))
            
        except Exception as e:
            logger.error(f"Failed to get distinct values: {str(e)}")
            return []