# app/services/ai_code_agent.py
from openai import OpenAI
from config.settings import get_settings
from typing import Dict, Any
import os
import logging
import re
import json  

logger = logging.getLogger(__name__)

# Clear any proxy environment variables
proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']
for var in proxy_vars:
    if var in os.environ:
        logger.info(f"Removing proxy environment variable: {var}")
        del os.environ[var]

settings = get_settings()
logger.info("AI Code Agent initializing...")

class AICodeAgent:
    """AI agent that generates pandas code for normalized data"""
    
    def __init__(self):
        try:
            self.client = OpenAI(api_key=settings.openai_api_key)
            logger.info("OpenAI client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {str(e)}")
            raise

    def generate_analysis_code(self, schema: Dict[str, Any], user_query: str) -> Dict[str, Any]:
        """Generate pandas code for multi-sheet queries"""
        logger.info(f"Starting code generation for query: {user_query}")
        
        try:
            is_multi_sheet = 'sheet_count' in schema and schema['sheet_count'] > 1
            
            if is_multi_sheet:
                return self._generate_multi_sheet_code(schema, user_query)
            else:
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
        """Generate code for multi-sheet analysis - ASSUMES NORMALIZED DATA"""
        logger.info("Generating multi-sheet analysis code")
        
        column_map = schema.get('column_to_sheet_map', {})
        sheet_names = schema.get('sheet_names', [])
        sheets_info = schema.get('sheets', {})
        
        # Build sheet summary
        sheet_summary = {}
        for sheet_name, sheet_schema in sheets_info.items():
            sheet_summary[sheet_name] = {
                'columns': sheet_schema['basic_info']['column_names'],
                'row_count': sheet_schema['basic_info']['total_rows'],
                'numerical_cols': sheet_schema['data_types'].get('numerical', []),
                'categorical_cols': sheet_schema['data_types'].get('categorical', []),
                'datetime_cols': sheet_schema['data_types'].get('datetime', [])
            }
        
        prompt = f"""
You are a pandas expert analyzing NORMALIZED multi-sheet Excel data.

USER QUERY: "{user_query}"

AVAILABLE SHEETS:
{json.dumps(sheet_summary, indent=2)}

CRITICAL: DATA IS PRE-NORMALIZED
1. Date columns are ALREADY datetime64 type - use standard datetime comparison
2. String columns are ALREADY cleaned (trimmed, no extra spaces)
3. Numeric columns are ALREADY proper int/float types
4. You can trust the data types - no need for complex parsing

DEFENSIVE CODING RULES (MANDATORY):
1. ALWAYS check if filter returns data before aggregating
2. ALWAYS handle empty results gracefully
3. NEVER assume data will match filters

MANDATORY PATTERN for aggregations:
filtered = df[conditions]
if len(filtered) == 0:
    query_result = 0  # or appropriate default
else:
    query_result = filtered['Column'].sum()

DATE FILTERING (SIMPLE - DATA IS NORMALIZED):
For datetime columns, use standard pandas datetime comparison:
df[df['OrderDate'].dt.date == pd.to_datetime('1/6/2024').date()]

STRING FILTERING (SIMPLE - DATA IS CLEANED):
For string columns, use case-insensitive matching:
df[df['Rep'].str.lower() == 'jones']

SCOPE VALIDATION:
If query references data not in the schema, respond with can_answer: false

TASK:
1. Determine which sheet(s) contain the data
2. Generate pandas code to answer the query
3. Store result in 'query_result'
4. Make result JSON-serializable

Return ONLY valid JSON:
{{
"can_answer": true/false,
"target_sheet": "SheetName" or null,
"requires_multiple_sheets": true/false,
"code": "pandas code" or null,
"explanation": "if cannot answer",
"missing_columns": []
}}

EXAMPLES WITH DEFENSIVE CODING:

Example 1 - Date filtering (DEFENSIVE):
Query: "pencils sold on 1/6/2024 by Jones"
Code:
df = sheets['Sheet1']
target_date = pd.to_datetime('1/6/2024').date()
filtered = df[(df['OrderDate'].dt.date == target_date) & 
              (df['Rep'].str.lower() == 'jones') & 
              (df['Item'].str.lower().str.strip() == 'pencil')]
if len(filtered) == 0:
    query_result = 0
else:
    query_result = int(filtered['Units'].sum())

Example 2 - Get dates from filtered results (DEFENSIVE):
Query: "which dates were pencils sold in East region"
Code:
df = sheets['Sheet1']
filtered = df[(df['Item'].str.lower().str.strip() == 'pencil') & 
              (df['Region'].str.lower() == 'east')]
if len(filtered) == 0:
    query_result = []
else:
    query_result = filtered['OrderDate'].dt.strftime('%Y-%m-%d').unique().tolist()

Example 3 - Best performing (DEFENSIVE):
Query: "best performing rep in east region"
Code:
df = sheets['Sheet1']
filtered = df[df['Region'].str.lower() == 'east']
if len(filtered) == 0:
    query_result = "No data found for East region"
else:
    query_result = filtered.groupby('Rep')['Total'].sum().idxmax()

Example 4 - Group by with date range (DEFENSIVE):
Query: "sales in January 2024"
Code:
df = sheets['Sheet1']
filtered = df[(df['OrderDate'].dt.year == 2024) & (df['OrderDate'].dt.month == 1)]
if len(filtered) == 0:
    query_result = 0.0
else:
    query_result = float(filtered['Total'].sum())

Example 5 - Multiple conditions (DEFENSIVE):
Query: "profit in Canada from Government segment in 2014"
Code:
df = sheets['Sheet2']
filtered = df[(df['Country'].str.lower() == 'canada') & 
              (df['Segment'].str.lower() == 'government') &
              (df['Date'].dt.year == 2014)]
if len(filtered) == 0:
    query_result = 0.0
else:
    query_result = float(filtered[' Profit '].sum())
"""
        
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=1500
        )
        
        raw_response = response.choices[0].message.content
        logger.info(f"Received code generation response")
        
        try:
            if "```json" in raw_response:
                raw_response = raw_response.split("```json")[1].split("```")[0]
            elif "```" in raw_response:
                raw_response = raw_response.split("```")[1].split("```")[0]
            
            result = json.loads(raw_response.strip())
            logger.info(f"Code generation result: can_answer={result.get('can_answer')}")
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse response as JSON: {str(e)}")
            return {
                'can_answer': False,
                'target_sheet': None,
                'requires_multiple_sheets': False,
                'code': None,
                'explanation': f"Error parsing response"
            }

    def _generate_single_sheet_code(self, schema: Dict[str, Any], user_query: str) -> str:
        """Generate code for single sheet - ASSUMES NORMALIZED DATA"""
        basic_info = schema.get('basic_info', {})
        columns = basic_info.get('column_names', [])
        data_types = schema.get('data_types', {})
        
        prompt = f"""
You are a pandas expert. Generate code for NORMALIZED data.

USER QUERY: "{user_query}"

DATASET INFO:
- Columns: {columns}
- Numerical: {data_types.get('numerical', [])}
- Categorical: {data_types.get('categorical', [])}
- Datetime: {data_types.get('datetime', [])}

DATA IS PRE-NORMALIZED:
1. Date columns are datetime64 - use dt.date, dt.year, dt.month
2. Strings are cleaned - use .str.lower() for case-insensitive
3. Numbers are proper types - no conversion needed

DEFENSIVE CODING (MANDATORY):
Always check if filter returns data before aggregating:
filtered = df[conditions]
if len(filtered) == 0:
    query_result = 0
else:
    query_result = filtered['Column'].sum()

EXAMPLES:

Query: "pencils sold on 1/6/2024 by Jones"
Code:
target_date = pd.to_datetime('1/6/2024').date()
filtered = df[(df['OrderDate'].dt.date == target_date) & 
              (df['Rep'].str.lower() == 'jones') & 
              (df['Item'].str.lower().str.strip() == 'pencil')]
if len(filtered) == 0:
    query_result = 0
else:
    query_result = int(filtered['Units'].sum())

Query: "best rep in east region"
Code:
filtered = df[df['Region'].str.lower() == 'east']
if len(filtered) == 0:
    query_result = "No data for East region"
else:
    query_result = filtered.groupby('Rep')['Total'].sum().idxmax()

Query: "which dates were pencils sold"
Code:
filtered = df[df['Item'].str.lower().str.strip() == 'pencil']
if len(filtered) == 0:
    query_result = []
else:
    query_result = filtered['OrderDate'].dt.strftime('%Y-%m-%d').unique().tolist()

Generate code that stores result in 'query_result'. Assume df is the dataframe.
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