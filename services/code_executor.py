# app/services/code_executor.py
import pandas as pd
import numpy as np
import sys
from io import StringIO
from typing import Dict, Any
import logging
import re
from typing import Dict, Any, Union


# Configure logging for this module
logger = logging.getLogger(__name__)



def sanitize_code(code: str) -> str:
    # Remove unsafe or redundant import statements
    code = re.sub(r"(?m)^\s*import\s+.*$", "", code)
    code = re.sub(r"(?m)^\s*from\s+.*import\s+.*$", "", code)
    return code
    
class SafeCodeExecutor:
    """Safely execute AI-generated pandas code"""
    
    def __init__(self):
        self.safe_builtins = {
            'len': len, 'str': str, 'int': int, 'float': float,
            'round': round, 'sum': sum, 'max': max, 'min': min,
            'abs': abs, 'sorted': sorted, 'list': list, 'dict': dict,
            'tuple': tuple, 'set': set, 'bool': bool, 'enumerate': enumerate,
            'range': range, 'zip': zip
        }
        logger.info("Safe Code Executor initialized with allowed builtins")
    





    def execute_code(self, df_or_sheets: Union[pd.DataFrame, Dict[str, pd.DataFrame]], code: str, target_sheet: str = None) -> Dict[str, Any]:
        """Execute AI-generated code safely - supports both single DF and multi-sheet dict"""
        
        # Determine if multi-sheet
        is_multi_sheet = isinstance(df_or_sheets, dict)
        
        if is_multi_sheet:
            logger.info(f"Starting multi-sheet code execution, {len(df_or_sheets)} sheets available")
            logger.debug(f"Sheet names: {list(df_or_sheets.keys())}")
        else:
            logger.info(f"Starting single-sheet code execution, dataframe shape: {df_or_sheets.shape}")
        
        logger.debug(f"Code to execute: {code}")
        
        # Validate code safety
        if not self._is_code_safe(code):
            logger.error("Code failed security validation")
            return {
                'success': False,
                'error': 'Code failed security validation',
                'result': None,
                'executed_code': code
            }
        
        # Create safe execution environment
        safe_builtins = {
            "str": str, "int": int, "float": float,
            "len": len, "range": range, "min": min,
            "max": max, "sum": sum, "abs": abs, "round": round,
        }

        safe_globals = {
            "__builtins__": safe_builtins,
            "pd": pd,
            "pandas": pd,
            "np": np,
        }
        
        # Inject data
        if is_multi_sheet:
            safe_globals["sheets"] = df_or_sheets
            # Also inject specific sheet if specified
            if target_sheet and target_sheet in df_or_sheets:
                safe_globals["df"] = df_or_sheets[target_sheet]
        else:
            safe_globals["df"] = df_or_sheets
        
        try:
            code = sanitize_code(code)
            exec(code, safe_globals)
            
            result = safe_globals.get('query_result')
            
            if result is not None:
                result = self._make_json_serializable(result)
            
            return {
                'success': True,
                'result': result,
                'executed_code': code
            }
            
        except Exception as e:
            logger.error(f"Code execution failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'result': None,
                'executed_code': code
            }
        

        
    def _is_code_safe(self, code: str) -> bool:
        """Basic safety validation"""
        logger.info("Performing code safety validation")
        
        dangerous_patterns = [
            'import os', 'import sys', 'import subprocess',
            'exec(', 'eval(', 'open(', 'file(',
            '__import__', 'globals(', 'locals(',
            'system', 'popen', 'subprocess'
        ]
        
        code_lower = code.lower()
        for pattern in dangerous_patterns:
            if pattern in code_lower:
                logger.warning(f"Dangerous pattern found in code: {pattern}")
                return False
        
        logger.info("Code passed safety validation")
        return True
    
    def _make_json_serializable(self, obj):
        """Convert pandas objects to JSON-serializable format, handling NaN values"""
        logger.info(f"Converting object to JSON-serializable format, input type: {type(obj)}")
        
        try:
            if isinstance(obj, dict):
                # Recursively handle dictionary values
                logger.info("Processing dictionary recursively")
                result = {}
                for key, value in obj.items():
                    # Handle pandas Period keys
                    if hasattr(key, '__str__') and not isinstance(key, (str, int, float)):
                        new_key = str(key)
                    else:
                        new_key = key
                    result[new_key] = self._make_json_serializable(value)
                logger.info("Successfully processed dictionary")
                return result
            elif isinstance(obj, (list, tuple)):
                # Recursively handle list/tuple items
                logger.info("Processing list/tuple recursively")
                result = [self._make_json_serializable(item) for item in obj]
                logger.info("Successfully processed list/tuple")
                return result
            elif isinstance(obj, (pd.Series, pd.Index)):
                logger.info("Converting pandas Series/Index to dict")
                # Convert to dict first, then recursively process
                dict_result = obj.to_dict()
                result = self._make_json_serializable(dict_result)
                logger.info("Successfully converted Series/Index to dict")
                return result
            elif isinstance(obj, pd.DataFrame):
                logger.info("Converting pandas DataFrame to dict")
                dict_result = obj.to_dict()
                result = self._make_json_serializable(dict_result)
                logger.info("Successfully converted DataFrame to dict")
                return result
            elif isinstance(obj, (np.integer, np.floating)):
                logger.info("Converting numpy number to float")
                result = float(obj)
                # Handle NaN values
                if np.isnan(result) or np.isinf(result):
                    logger.warning(f"Found NaN or Inf value, converting to None")
                    return None
                logger.info("Successfully converted numpy number to float")
                return result
            elif isinstance(obj, float):
                # Handle Python float NaN/Inf
                if np.isnan(obj) or np.isinf(obj):
                    logger.warning(f"Found NaN or Inf value, converting to None")
                    return None
                return obj
            elif isinstance(obj, np.ndarray):
                logger.info("Converting numpy array to list")
                result = obj.tolist()
                logger.info("Successfully converted numpy array to list")
                return result
            elif hasattr(obj, 'isoformat'):  # datetime objects
                logger.info("Converting datetime object to ISO format")
                result = obj.isoformat()
                logger.info("Successfully converted datetime to ISO format")
                return result
            elif hasattr(obj, '__str__') and 'pandas' in str(type(obj)):
                # Handle pandas Period and other pandas objects
                logger.info(f"Converting pandas object to string: {type(obj)}")
                result = str(obj)
                logger.info("Successfully converted pandas object to string")
                return result
            elif pd.isna(obj):  # Check for pandas NA values
                logger.warning("Found pandas NA value, converting to None")
                return None
            else:
                logger.info(f"Object already JSON-serializable, type: {type(obj)}")
                return obj
                
        except Exception as e:
            logger.error(f"Failed to convert object to JSON-serializable format: {str(e)}")
            logger.error(f"Object type: {type(obj)}, Object: {obj}")
            # Return None for problematic values instead of string
            return None
        