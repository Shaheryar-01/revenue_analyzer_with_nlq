# app/services/code_executor.py
import pandas as pd
import numpy as np
import sys
from io import StringIO
from typing import Dict, Any
import logging
import re


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
    





    def execute_code(self, df: pd.DataFrame, code: str) -> Dict[str, Any]:
        """Execute AI-generated code safely"""
        logger.info(f"Starting code execution, dataframe shape: {df.shape}")
        logger.debug(f"Code to execute: {code}")
        
        # Validate code safety
        logger.info("Validating code safety")
        if not self._is_code_safe(code):
            logger.error("Code failed security validation")
            return {
                'success': False,
                'error': 'Code failed security validation',
                'result': None,
                'executed_code': code
            }
        logger.info("Code passed security validation")
        
        # Create safe execution environment
        logger.info("Creating safe execution environment")
        safe_builtins = {
            "str": str,
            "int": int,
            "float": float,
            "len": len,
            "range": range,
            "min": min,
            "max": max,
            "sum": sum,
            "abs": abs,
            "round": round,
        }

        safe_globals = {
            "__builtins__": safe_builtins,
            "pd": pd,
            "pandas": pd,
            "np": np,
            "df": df,   # ✅ inject dataframe

        }
        logger.info("Safe execution environment created")
        
        try:
            # Execute code
            logger.info("Executing code")
            code = sanitize_code(code)
            exec(code, safe_globals)
            logger.info("Code executed successfully")
            
            result = safe_globals.get('query_result')
            logger.info(f"Retrieved query_result, type: {type(result)}")
            
            # Convert result to JSON-serializable format
            if result is not None:
                logger.info("Converting result to JSON-serializable format")
                result = self._make_json_serializable(result)
                logger.info(f"Result converted successfully, final type: {type(result)}")
            else:
                logger.warning("query_result is None after code execution")
            
            return {
                'success': True,
                'result': result,
                'executed_code': code
            }
            
        except Exception as e:
            logger.error(f"Code execution failed: {str(e)}", exc_info=True)
            logger.error(f"Failed code: {code}")
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
        """Convert pandas objects to JSON-serializable format"""
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
                logger.info("Successfully converted numpy number to float")
                return result
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
            else:
                logger.info(f"Object already JSON-serializable, type: {type(obj)}")
                return obj
                
        except Exception as e:
            logger.error(f"Failed to convert object to JSON-serializable format: {str(e)}")
            logger.error(f"Object type: {type(obj)}, Object: {obj}")
            # Return string representation as fallback
            return str(obj)