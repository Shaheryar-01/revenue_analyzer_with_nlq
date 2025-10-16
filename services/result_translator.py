"""
Translate technical results to business-friendly language
"""
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

class ResultTranslator:
    """Translates technical output to human-readable format"""
    
    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.column_patterns = schema.get('column_patterns', {})
        self.month_map = self._build_month_map()
    
    def _build_month_map(self) -> Dict[str, str]:
        """Build mapping from column names to human-readable months"""
        month_map = {}
        
        sequential_groups = self.column_patterns.get('sequential_groups', [])
        for group in sequential_groups:
            if group['count'] == 12:  # Monthly pattern
                base = group['base_name']
                columns = group['columns']
                
                month_names = [
                    'January', 'February', 'March', 'April', 'May', 'June',
                    'July', 'August', 'September', 'October', 'November', 'December'
                ]
                
                for i, col in enumerate(columns):
                    month_map[col] = month_names[i]
                    logger.info(f"Mapped {col} → {month_names[i]}")
        
        logger.info(f"Built month map with {len(month_map)} entries")
        return month_map
    
    def translate_result(self, result: Any, query: str) -> Any:
        """
        Translate technical result to business-friendly format
        
        Args:
            result: Raw result from code execution
            query: Original user query
            
        Returns:
            Translated result
        """
        try:
            # Handle dict results (from "which period" queries)
            if isinstance(result, dict):
                # Check if it's a period result
                if 'column' in result and 'value' in result:
                    return self._translate_period_result(result)
                
                # Check if it's a ranking/groupby result with column names as keys
                translated_dict = {}
                has_column_names = False
                
                for key, value in result.items():
                    # Check if key is a column name that needs translation
                    if isinstance(key, str) and key in self.month_map:
                        translated_dict[self.month_map[key]] = value
                        has_column_names = True
                    else:
                        translated_dict[key] = value
                
                if has_column_names:
                    logger.info(f"Translated dict keys: {translated_dict}")
                    return translated_dict
            
            # No translation needed
            return result
            
        except Exception as e:
            logger.error(f"Failed to translate result: {str(e)}", exc_info=True)
            return result
    
    def _translate_period_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Translate column name to human-readable period"""
        column = result.get('column', '')
        value = result.get('value', 0)
        
        logger.info(f"Translating period result: column={column}, value={value}")
        
        # Try to translate column name to month using pre-built map
        if column in self.month_map:
            translated = {
                'period': self.month_map[column],
                'column': column,
                'value': value
            }
            logger.info(f"Translated using month_map: {translated}")
            return translated
        
        # Fallback: try to extract month number from column name
        # e.g., "Actual.6" → "Month 7" (because .6 is 7th month, 0-indexed)
        if '.' in column:
            try:
                base, suffix = column.rsplit('.', 1)
                month_num = int(suffix) + 1  # Convert 0-indexed to 1-indexed
                month_names = [
                    'January', 'February', 'March', 'April', 'May', 'June',
                    'July', 'August', 'September', 'October', 'November', 'December'
                ]
                if 0 <= month_num - 1 < 12:
                    translated = {
                        'period': month_names[month_num - 1],
                        'column': column,
                        'value': value
                    }
                    logger.info(f"Translated using suffix parsing: {translated}")
                    return translated
            except (ValueError, IndexError) as e:
                logger.warning(f"Failed to parse column suffix: {e}")
        elif not '.' in column:
            # Base column (e.g., "Actual") = first month
            translated = {
                'period': 'January',
                'column': column,
                'value': value
            }
            logger.info(f"Translated base column as January: {translated}")
            return translated
        
        # Fallback: return as-is
        logger.warning(f"Could not translate column {column}, returning as-is")
        return result