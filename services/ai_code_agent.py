# app/services/ai_code_agent.py
from openai import OpenAI
from config.settings import get_settings
from typing import Dict, Any
import os
import logging
import re
import json  




# Configure logging for this module
logger = logging.getLogger(__name__)

# Clear any proxy environment variables that might interfere
proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']
for var in proxy_vars:
    if var in os.environ:
        logger.info(f"Removing proxy environment variable: {var}")
        del os.environ[var]

settings = get_settings()
logger.info("AI Code Agent initializing...")

class AICodeAgent:
    """AI agent that generates pandas code for user queries"""
    
    def __init__(self):
        try:
            self.client = OpenAI(api_key=settings.openai_api_key)
            logger.info("OpenAI client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {str(e)}")
            raise
    




    def generate_analysis_code(self, schema: Dict[str, Any], user_query: str) -> Dict[str, Any]:
        """Generate pandas code for multi-sheet queries with intelligent routing"""
        logger.info(f"Starting multi-sheet code generation for query: {user_query}")
        
        try:
            # Check if this is multi-sheet schema
            is_multi_sheet = 'sheet_count' in schema and schema['sheet_count'] > 1
            
            if is_multi_sheet:
                return self._generate_multi_sheet_code(schema, user_query)
            else:
                # Single sheet - use existing logic
                code = self._generate_single_sheet_code(schema, user_query)
                return {
                    'code': code,
                    'target_sheet': None,
                    'requires_multiple_sheets': False
                }
                
        except Exception as e:
            logger.error(f"Failed to generate analysis code: {str(e)}", exc_info=True)
            raise

    def _generate_multi_sheet_code(self, schema: Dict[str, Any], user_query: str) -> Dict[str, Any]:
        """Generate code for multi-sheet analysis"""
        logger.info("Generating multi-sheet analysis code")
        
        column_map = schema.get('column_to_sheet_map', {})
        sheet_names = schema.get('sheet_names', [])
        sheets_info = schema.get('sheets', {})
        
        # Build concise sheet info for LLM
        sheet_summary = {}
        for sheet_name, sheet_schema in sheets_info.items():
            sheet_summary[sheet_name] = {
                'columns': sheet_schema['basic_info']['column_names'],
                'row_count': sheet_schema['basic_info']['total_rows'],
                'numerical_cols': sheet_schema['data_types'].get('numerical', []),
                'categorical_cols': sheet_schema['data_types'].get('categorical', [])
            }
        
        prompt = f"""
    You are a pandas expert analyzing multi-sheet Excel data.

    USER QUERY: "{user_query}"

    AVAILABLE SHEETS:
    {json.dumps(sheet_summary, indent=2)}

    COLUMN TO SHEET MAPPING:
    {json.dumps(column_map, indent=2)}

    TASK:
    1. Determine which sheet(s) contain the data needed for this query
    2. Generate Python pandas code to answer the query
    3. If all required columns are in ONE sheet, use: df = sheets['SheetName']
    4. If columns span MULTIPLE sheets, explain which columns are in which sheets
    5. Store result in variable 'query_result'
    6. Make result JSON-serializable

    Return ONLY valid JSON in this format:
    {{
    "can_answer": true/false,
    "target_sheet": "SheetName" or null,
    "requires_multiple_sheets": true/false,
    "code": "pandas code here" or null,
    "explanation": "explanation if cannot answer or requires multiple sheets",
    "missing_columns": [] if applicable
    }}

    EXAMPLES:
    - If query asks for "profit in Canada" and both are in "Revenue" sheet:
    {{"can_answer": true, "target_sheet": "Revenue", "requires_multiple_sheets": false, "code": "df = sheets['Revenue']\\nquery_result = df[df['Country'] == 'Canada']['Profit'].sum()", "explanation": ""}}

    - If "profit" is in Sheet2 but "region" is in Sheet1:
    {{"can_answer": false, "target_sheet": null, "requires_multiple_sheets": true, "code": null, "explanation": "Profit exists in Sheet2, but Region only exists in Sheet1. These sheets don't share this data directly."}}
    """
        
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=1500
        )
        
        raw_response = response.choices[0].message.content
        logger.info(f"Received multi-sheet routing response")
        
        # Parse JSON response
        try:
            # Clean up response if wrapped in markdown
            if "```json" in raw_response:
                raw_response = raw_response.split("```json")[1].split("```")[0]
            elif "```" in raw_response:
                raw_response = raw_response.split("```")[1].split("```")[0]
            
            result = json.loads(raw_response.strip())
            logger.info(f"Multi-sheet analysis result: can_answer={result.get('can_answer')}, target_sheet={result.get('target_sheet')}")
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {str(e)}")
            # Fallback
            return {
                'can_answer': False,
                'target_sheet': None,
                'requires_multiple_sheets': False,
                'code': None,
                'explanation': f"Error parsing response: {raw_response[:200]}"
            }

    def _generate_single_sheet_code(self, schema: Dict[str, Any], user_query: str) -> str:
        """Generate code for single sheet (original logic)"""
        # Your existing generate_analysis_code logic here
        basic_info = schema.get('basic_info', {})
        columns = basic_info.get('column_names', [])
        data_types = schema.get('data_types', {})
        
        prompt = f"""
    You are a pandas expert. Generate Python code to answer this user query.

    USER QUERY: "{user_query}"

    DATASET INFO:
    - Columns: {columns}
    - Numerical columns: {data_types.get('numerical', [])}
    - Categorical columns: {data_types.get('categorical', [])}

    Generate pandas code that stores result in 'query_result'. Assume dataframe is called 'df'.
    Return ONLY the code, no explanations.
    """
        
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        
        code = response.choices[0].message.content
        if "```python" in code:
            code = code.split("```python")[1].split("```")[0]
        elif "```" in code:
            code = code.split("```")[1].split("```")[0]
        
        return code.strip()
