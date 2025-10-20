# app/services/excel_transformer.py
import pandas as pd
import logging
from typing import List, Dict, Any
import uuid

logger = logging.getLogger(__name__)

class ExcelTransformer:
    """Transform Excel data from wide format to long format for database"""
    
    def __init__(self):
        self.months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                      'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        logger.info("Excel Transformer initialized")
    
    def transform_excel_to_records(self, file_path: str, upload_id: str) -> List[Dict[str, Any]]:
        """
        Transform Excel file from wide format to long format
        
        Args:
            file_path: Path to uploaded Excel file
            upload_id: Unique identifier for this upload
            
        Returns:
            List of records ready for database insertion
        """
        try:
            logger.info(f"Reading Excel file: {file_path}")
            
            # Read first sheet only, skip first row (merged headers)
            df = pd.read_excel(file_path, sheet_name=0, header=1)
            
            logger.info(f"Loaded sheet with shape: {df.shape}")
            logger.info(f"Columns: {list(df.columns)[:10]}")
            
            # Clean column names
            df.columns = df.columns.str.strip()
            
            # Drop rows that are summary/aggregate rows
            df = df[df['Unit'].notna()]
            df = df[df['Unit'] != '=']
            df = df[df['Unit'].str.strip() != '']
            
            # Remove aggregate rows (rows where Product is '=')
            df = df[df['Product'] != '=']
            
            logger.info(f"After basic filtering: {len(df)} rows")
            
            # 🔥 NEW: Filter out TOTAL rows
            df = self._filter_total_rows(df)
            
            logger.info(f"After filtering totals: {len(df)} data rows")
            
            # Transform to long format
            records = self._transform_to_long_format(df, upload_id)
            
            logger.info(f"Transformed to {len(records)} database records")
            
            return records
            
        except Exception as e:
            logger.error(f"Failed to transform Excel: {str(e)}", exc_info=True)
            raise
    
    def _filter_total_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter out summary/total rows from dataframe.
        
        Total rows are identified by:
        - Unit containing "Total", "Region", "CISO"
        - Product containing "Total"
        - Category being empty when Unit has "Total"
        """
        
        initial_count = len(df)
        
        # Create masks for total rows
        unit_total_mask = df['Unit'].str.contains('Total', case=False, na=False)
        unit_region_mask = df['Unit'].str.contains('Region', case=False, na=False)
        unit_ciso_mask = df['Unit'].str.contains('CISO', case=False, na=False)
        product_total_mask = df['Product'].str.contains('Total', case=False, na=False)
        
        # Combine masks - keep rows that are NOT totals
        is_total_row = unit_total_mask | unit_region_mask | unit_ciso_mask | product_total_mask
        
        # Filter out total rows
        df_filtered = df[~is_total_row].copy()
        
        removed_count = initial_count - len(df_filtered)
        logger.info(f"Filtered out {removed_count} total/summary rows")
        
        # Log some examples of what was filtered
        if removed_count > 0:
            total_rows = df[is_total_row][['Unit', 'Product']].head(5)
            logger.info(f"Examples of filtered rows:\n{total_rows.to_string()}")
        
        return df_filtered
    
    def _transform_to_long_format(self, df: pd.DataFrame, upload_id: str) -> List[Dict[str, Any]]:
        """
        Transform wide format to long format
        
        Input: 1 row with 12 months × 3 metrics = 36 columns
        Output: 12 rows (1 per month) with 3 metric columns
        """
        records = []
        
        for idx, row in df.iterrows():
            # Get dimension values (same for all months)
            base_record = {
                'upload_id': upload_id,
                'unit': self._clean_value(row.get('Unit')),
                'product': self._clean_value(row.get('Product')),
                'region': self._clean_value(row.get('Region')),
                'country': self._clean_value(row.get('Country')),
                'customer': self._clean_value(row.get('Customer')),
                'category': self._clean_value(row.get('Category')),
                'project_code': self._clean_value(row.get('Project Code')),
                'year': 2025,
                
                # Summary columns (same for all months)
                'ytd_actual': self._clean_numeric(row.get('YTD Actual')),
                'remaining_projection': self._clean_numeric(row.get('Remaining Projection')),
                'total': self._clean_numeric(row.get('Total')),
                'wih_2024': self._clean_numeric(row.get('WIH 2024')),
                'advance_2025': self._clean_numeric(row.get('Advance 2025')),
                'wih_2025': self._clean_numeric(row.get('WIH 2025')),
                'on_hold': self._clean_numeric(row.get('on hold ')),
                'wih_2026': self._clean_numeric(row.get('WIH 2026')),
                'shelved': self._clean_numeric(row.get('Shelved '))
            }
            
            # Create one record per month
            for month_idx, month in enumerate(self.months):
                month_record = base_record.copy()
                month_record['month'] = month
                
                # Get metrics for this month
                # The pattern is: Budget (col 7), Projected (col 8), Actual (col 9) for JAN
                # Then Budget (col 10), Projected (col 11), Actual (col 12) for FEB, etc.
                
                # Calculate column positions
                base_col_idx = 7 + (month_idx * 3)  # Starting position for this month
                
                # Get column names dynamically
                try:
                    cols = list(df.columns)
                    budget_col = cols[base_col_idx] if base_col_idx < len(cols) else None
                    projected_col = cols[base_col_idx + 1] if base_col_idx + 1 < len(cols) else None
                    actual_col = cols[base_col_idx + 2] if base_col_idx + 2 < len(cols) else None
                    
                    month_record['budget'] = self._clean_numeric(row.get(budget_col)) if budget_col else None
                    month_record['projected'] = self._clean_numeric(row.get(projected_col)) if projected_col else None
                    month_record['actual'] = self._clean_numeric(row.get(actual_col)) if actual_col else None
                    
                except Exception as e:
                    logger.warning(f"Failed to get metrics for {month}: {str(e)}")
                    month_record['budget'] = None
                    month_record['projected'] = None
                    month_record['actual'] = None
                
                records.append(month_record)
        
        return records
    
    def _clean_value(self, value) -> str:
        """Clean string values"""
        if pd.isna(value) or value is None:
            return None
        
        value_str = str(value).strip()
        
        # Handle '=' as null
        if value_str == '=' or value_str == '':
            return None
        
        return value_str
    
    def _clean_numeric(self, value) -> float:
        """Clean numeric values"""
        if pd.isna(value) or value is None:
            return None
        
        # Handle '=' as null
        if value == '=' or value == '':
            return None
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def validate_excel_structure(self, file_path: str) -> Dict[str, Any]:
        """
        Validate that Excel file has the expected structure
        
        Returns:
            Validation result with success flag and message
        """
        try:
            df = pd.read_excel(file_path, sheet_name=0, header=1, nrows=5)
            
            required_columns = [
                'Unit', 'Product', 'Region', 'Country', 'Customer', 'Category', 'Project Code'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                return {
                    'valid': False,
                    'message': f'Missing required columns: {", ".join(missing_columns)}'
                }
            
            return {
                'valid': True,
                'message': 'File structure is valid'
            }
            
        except Exception as e:
            return {
                'valid': False,
                'message': f'Failed to validate file: {str(e)}'
            }