
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
        
        # Build sheet summary WITH SAMPLE VALUES
        sheet_summary = {}
        for sheet_name, sheet_schema in sheets_info.items():
            columns_info = sheet_schema.get('columns', {})
            
            # Get sample values for key columns
            unit_samples = columns_info.get('Unit', {}).get('sample_values', [])[:10]
            region_samples = columns_info.get('Region', {}).get('sample_values', [])[:10]
            category_samples = columns_info.get('Category', {}).get('sample_values', [])[:10]
            
            sheet_summary[sheet_name] = {
                'columns': sheet_schema['basic_info']['column_names'],
                'row_count': sheet_schema['basic_info']['total_rows'],
                'numerical_cols': sheet_schema['data_types'].get('numerical', []),
                'categorical_cols': sheet_schema['data_types'].get('categorical', []),
                'datetime_cols': sheet_schema['data_types'].get('datetime', []),
                'unit_samples': unit_samples,
                'region_samples': region_samples,
                'category_samples': category_samples
            }
        
        prompt = f"""
    You are a pandas expert analyzing NORMALIZED multi-sheet Excel data.

    USER QUERY: "{user_query}"

    AVAILABLE SHEETS:
    {json.dumps(sheet_summary, indent=2)}

    CRITICAL COLUMN IDENTIFICATION RULES:
    1. Unit column contains business units: "ME Support", "AMS", "Sales Force", "BSD", etc.
    2. Region column contains geographic regions: "ME", "AFR", "PK"
    3. Category column contains project types: "Funnel", "New Sales", "WIH"

    QUERY-TO-COLUMN MAPPING:
    - If user mentions "ME Support", "AMS", "Sales Force" → Filter by Unit column
    - If user mentions "ME region", "AFR", "PK" (without "Support") → Filter by Region column  
    - If user mentions "Funnel", "New Sales", "WIH" → Filter by Category column

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
    - Exact match: df[df['Region'].str.lower() == 'me']
    - Contains match: df[df['Unit'].str.lower().str.contains('me support', na=False)]
    CRITICAL: Always use na=False in .str.contains() to handle NaN values

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

    EXAMPLES WITH CORRECT COLUMN SELECTION:

    Example 1 - Business Unit Query:
    Query: "budget for ME Support"
    Code:
    df = sheets['Sheet1']
    filtered = df[df['Unit'].str.lower().str.contains('me support', na=False)]
    if len(filtered) == 0:
        query_result = 0
    else:
        query_result = float(filtered['Budget'].sum())

    Example 2 - Region Query:
    Query: "budget for ME region"
    Code:
    df = sheets['Sheet1']
    filtered = df[df['Region'].str.lower() == 'me']
    if len(filtered) == 0:
        query_result = 0
    else:
        query_result = float(filtered['Budget'].sum())

    Example 3 - Another Business Unit:
    Query: "total budget for Sales Force"
    Code:
    df = sheets['Sheet1']
    filtered = df[df['Unit'].str.lower().str.contains('sales force', na=False)]
    if len(filtered) == 0:
        query_result = 0
    else:
        query_result = float(filtered['Budget'].sum())

    Example 4 - Category Query:
    Query: "budget for New Sales category"
    Code:
    df = sheets['Sheet1']
    filtered = df[df['Category'].str.lower() == 'new sales']
    if len(filtered) == 0:
        query_result = 0
    else:
        query_result = float(filtered['Budget'].sum())
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


    def _get_analytical_templates(self, column_patterns: Dict[str, Any]) -> str:
        """Generate code templates for common analytical patterns"""
        
        templates = """
    📋 **COMMON ANALYTICAL QUERY PATTERNS:**

    These are tested, working code patterns. Use these templates when applicable.

    ---

    **PATTERN 1: "Which [entity] has highest/lowest [metric] overall?"**
    Example: "Which customer generated most revenue?"
    Example: "Which product has lowest budget?"

    Template:
    ```python
    # Get all metric columns (e.g., all 12 Actual columns)
    metric_columns = [col for col in df.columns if col.startswith('[MetricName]')]

    # Group by entity and sum across all months
    entity_totals = df.groupby('[EntityColumn]')[metric_columns].sum().sum(axis=1)

    # Find the entity with max/min
    if len(entity_totals) == 0:
        query_result = "No data found"
    else:
        top_entity = entity_totals.idxmax()  # or .idxmin() for lowest
        top_value = entity_totals.max()
        query_result = f"{top_entity}: {top_value}"
    ```

    Real example:
    ```python
    # Which customer generated most revenue?
    actual_columns = ['Actual', 'Actual.1', 'Actual.2', 'Actual.3', 'Actual.4', 
                    'Actual.5', 'Actual.6', 'Actual.7', 'Actual.8', 'Actual.9', 
                    'Actual.10', 'Actual.11']
    customer_totals = df.groupby('Customer')[actual_columns].sum().sum(axis=1)
    if len(customer_totals) == 0:
        query_result = "No data found"
    else:
        top_customer = customer_totals.idxmax()
        top_value = float(customer_totals.max())
        query_result = f"{top_customer}: ${top_value:,.2f}"
    ```

    ---

    **PATTERN 2: "Which [entity] has highest [metric] in [specific period]?"**
    Example: "Which country has highest revenue in March?"
    Example: "Which product performed best in Q1?"

    Template:
    ```python
    # Identify the column(s) for the period
    period_columns = ['ColumnForPeriod']  # e.g., ['Actual.2'] for March

    # Group by entity and sum the period column(s)
    entity_period = df.groupby('[EntityColumn]')[period_columns].sum().sum(axis=1)

    # Find the entity with max value
    if len(entity_period) == 0:
        query_result = "No data found"
    else:
        top_entity = entity_period.idxmax()
        top_value = entity_period.max()
        query_result = f"{top_entity}: {top_value}"
    ```

    Real example:
    ```python
    # Which country has highest revenue in March?
    march_column = ['Actual.2']  # March = 3rd month = .2 suffix
    country_march = df.groupby('Country')[march_column].sum().sum(axis=1)
    if len(country_march) == 0:
        query_result = "No data found"
    else:
        top_country = country_march.idxmax()
        top_value = float(country_march.max())
        query_result = f"{top_country}: ${top_value:,.2f}"
    ```

    ---

    **PATTERN 3: "Which [period] had highest [metric]?"**
    Example: "Which month had highest revenue?"
    Example: "What was our best performing quarter?"

    Template:
    ```python
    # Get all columns for the metric
    metric_columns = [col for col in df.columns if col.startswith('[MetricName]')]

    # Sum each column (each represents a time period)
    period_totals = df[metric_columns].sum()

    # Find the period with max value
    if len(period_totals) == 0:
        query_result = "No data found"
    else:
        top_period_col = period_totals.idxmax()
        top_value = period_totals.max()
        # Convert column name to human-readable format
        query_result = {"column": top_period_col, "value": float(top_value)}
    ```

    Real example:
    ```python
    # Which month had highest actual revenue?
    actual_columns = ['Actual', 'Actual.1', 'Actual.2', 'Actual.3', 'Actual.4', 
                    'Actual.5', 'Actual.6', 'Actual.7', 'Actual.8', 'Actual.9', 
                    'Actual.10', 'Actual.11']
    month_totals = df[actual_columns].sum()
    if len(month_totals) == 0:
        query_result = "No data found"
    else:
        top_month_col = month_totals.idxmax()
        top_value = float(month_totals.max())
        query_result = {"column": top_month_col, "value": top_value}
    ```

    ---

    **PATTERN 4: "Rank [entities] by [metric]"**
    Example: "Rank customers by total revenue"
    Example: "Top 5 products by budget"

    Template:
    ```python
    # Get all metric columns
    metric_columns = [col for col in df.columns if col.startswith('[MetricName]')]

    # Group and sum
    entity_totals = df.groupby('[EntityColumn]')[metric_columns].sum().sum(axis=1)

    # Sort and optionally limit
    entity_ranked = entity_totals.sort_values(ascending=False)
    if '[limit]' in query:  # e.g., "top 5"
        entity_ranked = entity_ranked.head(5)

    query_result = entity_ranked.to_dict()
    ```

    Real example:
    ```python
    # Top 5 customers by revenue
    actual_columns = ['Actual', 'Actual.1', 'Actual.2', 'Actual.3', 'Actual.4', 
                    'Actual.5', 'Actual.6', 'Actual.7', 'Actual.8', 'Actual.9', 
                    'Actual.10', 'Actual.11']
    customer_totals = df.groupby('Customer')[actual_columns].sum().sum(axis=1)
    customer_ranked = customer_totals.sort_values(ascending=False).head(5)
    query_result = customer_ranked.to_dict()
    ```

    ---

    **CRITICAL NOTES:**
    - ALWAYS use .sum(axis=1) after groupby to sum across columns horizontally
    - ALWAYS check if result is empty before calling .idxmax() or .idxmin()
    - For "which period" queries, return BOTH column name and value as a dict
    - Use float() to convert numpy types to Python types
    """
        
        # Add detected patterns to templates
        sequential_groups = column_patterns.get('sequential_groups', [])
        if sequential_groups:
            templates += "\n\n**YOUR DATASET HAS THESE PATTERNS:**\n"
            for group in sequential_groups:
                templates += f"\n- {group['base_name']}: {group['columns']}\n"
        
        return templates


    def _generate_single_sheet_code(self, schema: Dict[str, Any], user_query: str) -> str:
        """Generate code for single sheet - ASSUMES NORMALIZED DATA"""
        basic_info = schema.get('basic_info', {})
        columns = basic_info.get('column_names', [])
        data_types = schema.get('data_types', {})
        
        # Get sample values for key columns
        column_samples = schema.get('columns', {})
        unit_samples = column_samples.get('Unit', {}).get('sample_values', [])[:10]
        region_samples = column_samples.get('Region', {}).get('sample_values', [])[:10]
        category_samples = column_samples.get('Category', {}).get('sample_values', [])[:10]
        
        # Get pattern context
        column_patterns = schema.get('column_patterns', {})
        pattern_context = ""
        
        if column_patterns.get('has_patterns'):
            pattern_context = f"""
    🔍 **DETECTED COLUMN PATTERNS:**

    {column_patterns.get('summary', '')}

    **HOW TO USE PATTERNS IN CODE:**
    """
            
            for group in column_patterns.get('sequential_groups', []):
                base = group['base_name']
                cols = group['columns']
                
                if group['count'] == 12:
                    pattern_context += f"""
    {base} Pattern (12 monthly columns):
    - Column list: {cols}
    - January = {cols[0]}
    - February = {cols[1]}
    - March = {cols[2]}
    - Q1 = sum of [{cols[0]}, {cols[1]}, {cols[2]}]
    - Q2 = sum of [{cols[3]}, {cols[4]}, {cols[5]}]
    - Q3 = sum of [{cols[6]}, {cols[7]}, {cols[8]}]
    - Q4 = sum of [{cols[9]}, {cols[10]}, {cols[11]}]
    - Full year = sum of all 12 columns

    CODE EXAMPLES:
    - "March budget" → df['{cols[2]}'].sum()
    - "Q1 budget" → df[['{cols[0]}', '{cols[1]}', '{cols[2]}']].sum().sum()
    - "Full year budget" → df[{cols}].sum().sum()
    """
                elif group['count'] == 4:
                    pattern_context += f"""
    {base} Pattern (4 quarterly columns):
    - Column list: {cols}
    - Q1 = {cols[0]}
    - Q2 = {cols[1]}
    - Q3 = {cols[2]}
    - Q4 = {cols[3]}

    CODE EXAMPLE:
    - "Q1 {base.lower()}" → df['{cols[0]}'].sum()
    """
        
        # ✅ ADD ANALYTICAL TEMPLATES
        analytical_templates = self._get_analytical_templates(column_patterns)
        
        prompt = f"""
    You are a pandas expert. Generate code for NORMALIZED data.

    {pattern_context}

    {analytical_templates}

    USER QUERY: "{user_query}"

    DATASET INFO:
    - Columns: {columns}
    - Numerical: {data_types.get('numerical', [])}
    - Categorical: {data_types.get('categorical', [])}
    - Datetime: {data_types.get('datetime', [])}

    CRITICAL COLUMN IDENTIFICATION:
    - Unit column samples: {unit_samples}
    → Contains business units like "ME Support", "AMS", "Sales Force", etc.
    - Region column samples: {region_samples}
    → Contains geographic regions like "ME", "AFR", "PK"
    - Category column samples: {category_samples}
    → Contains project categories like "Funnel", "New Sales", "WIH"

    IMPORTANT RULES:
    1. **Always check the ANALYTICAL TEMPLATES first** - if the query matches a pattern, USE THAT TEMPLATE
    2. For "which X has highest Y" queries, use PATTERN 1 or PATTERN 2 templates
    3. For "which month/period" queries, use PATTERN 3 template and return a dict
    4. ALWAYS use .sum(axis=1) after groupby when summing multiple columns
    5. ALWAYS check if result is empty before using .idxmax() or .idxmin()

    DATA IS PRE-NORMALIZED:
    1. Date columns are datetime64 - use dt.date, dt.year, dt.month
    2. Strings are cleaned - use .str.lower() for case-insensitive matching
    3. Numbers are proper types - no conversion needed
    4. CRITICAL: Always use na=False in .str.contains() to handle NaN values

    DEFENSIVE CODING (MANDATORY):
    Always check if filter returns data before aggregating:
    filtered = df[conditions]
    if len(filtered) == 0:
        query_result = 0
    else:
        query_result = filtered['Column'].sum()

    NOW GENERATE CODE:
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
