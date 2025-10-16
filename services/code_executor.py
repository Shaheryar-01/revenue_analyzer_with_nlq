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
    """Safely execute AI-generated pandas code with full observability"""
    
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
        """Execute AI-generated code safely with comprehensive logging"""
        
        # Determine if multi-sheet
        is_multi_sheet = isinstance(df_or_sheets, dict)
        
        # LOGGING: Data structure info
        logger.info(f"=" * 80)
        logger.info("CODE EXECUTION START")
        logger.info(f"=" * 80)
        
        if is_multi_sheet:
            logger.info(f"Multi-sheet execution: {list(df_or_sheets.keys())}")
            for sheet_name, df in df_or_sheets.items():
                logger.info(f"  Sheet '{sheet_name}':")
                logger.info(f"    Shape: {df.shape}")
                logger.info(f"    Columns: {list(df.columns)}")
                logger.info(f"    Column dtypes:")
                for col in df.columns:
                    logger.info(f"      {col}: {df[col].dtype}")
        else:
            logger.info(f"Single sheet execution:")
            logger.info(f"  Shape: {df_or_sheets.shape}")
            logger.info(f"  Columns: {list(df_or_sheets.columns)}")
            logger.info(f"  Column dtypes:")
            for col in df_or_sheets.columns:
                logger.info(f"    {col}: {df_or_sheets[col].dtype}")
        
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
    "__import__": __import__,
    "any": any,      
    "all": all,      
    "zip": zip,      
    "enumerate": enumerate,  
    "sorted": sorted,   
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
            if target_sheet and target_sheet in df_or_sheets:
                safe_globals["df"] = df_or_sheets[target_sheet]
        else:
            safe_globals["df"] = df_or_sheets
        
        try:
            code = sanitize_code(code)
            
            logger.info("=" * 50)
            logger.info("EXECUTING CODE:")
            logger.info(code)
            logger.info("=" * 50)
            
            exec(code, safe_globals)
            
            result = safe_globals.get('query_result')
            
            # LOGGING: Execution details
            logger.info(f"✅ Execution successful")
            logger.info(f"Result type: {type(result)}")
            logger.info(f"Result value: {result}")
            
            # Check if result makes sense
            if result is None:
                logger.warning("⚠️  query_result is None - code may not have set it")
            
            if result is not None:
                result = self._make_json_serializable(result)
            
            logger.info(f"=" * 80)
            logger.info("CODE EXECUTION COMPLETE")
            logger.info(f"=" * 80)
            
            return {
                'success': True,
                'result': result,
                'executed_code': code
            }
            
        except Exception as e:
            logger.error(f"=" * 80)
            logger.error(f"❌ CODE EXECUTION FAILED ❌")
            logger.error(f"=" * 80)
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error message: {str(e)}")
            logger.error(f"Code that failed:")
            logger.error(code)
            logger.error(f"Full traceback:", exc_info=True)
            
            # Provide helpful context
            error_str = str(e)
            if "does not support reduction" in error_str:
                logger.error("⚠️  LIKELY CAUSE: Trying to aggregate wrong column type (e.g., datetime instead of numeric)")
                logger.error("This usually means:")
                logger.error("  1. Filter returned 0 rows (empty DataFrame)")
                logger.error("  2. Wrong column was selected after filtering")
                logger.error("  3. Column name has extra spaces")
            elif "KeyError" in error_str:
                logger.error("⚠️  LIKELY CAUSE: Column doesn't exist")
                if is_multi_sheet:
                    logger.error(f"Available columns: {list(df_or_sheets[list(df_or_sheets.keys())[0]].columns)}")
                else:
                    logger.error(f"Available columns: {list(df_or_sheets.columns)}")
            
            logger.error(f"=" * 80)
            
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
        logger.debug(f"Converting object to JSON-serializable format, input type: {type(obj)}")
        
        try:
            if isinstance(obj, dict):
                result = {}
                for key, value in obj.items():
                    if hasattr(key, '__str__') and not isinstance(key, (str, int, float)):
                        new_key = str(key)
                    else:
                        new_key = key
                    result[new_key] = self._make_json_serializable(value)
                return result
            elif isinstance(obj, (list, tuple)):
                result = [self._make_json_serializable(item) for item in obj]
                return result
            elif isinstance(obj, (pd.Series, pd.Index)):
                dict_result = obj.to_dict()
                result = self._make_json_serializable(dict_result)
                return result
            elif isinstance(obj, pd.DataFrame):
                dict_result = obj.to_dict()
                result = self._make_json_serializable(dict_result)
                return result
            elif isinstance(obj, (np.integer, np.floating)):
                result = float(obj)
                if np.isnan(result) or np.isinf(result):
                    return None
                return result
            elif isinstance(obj, float):
                if np.isnan(obj) or np.isinf(obj):
                    return None
                return obj
            elif isinstance(obj, np.ndarray):
                result = obj.tolist()
                return result
            elif hasattr(obj, 'isoformat'):
                result = obj.isoformat()
                return result
            elif hasattr(obj, '__str__') and 'pandas' in str(type(obj)):
                result = str(obj)
                return result
            elif pd.isna(obj):
                return None
            else:
                return obj
                
        except Exception as e:
            logger.error(f"Failed to convert object to JSON-serializable format: {str(e)}")
            return None