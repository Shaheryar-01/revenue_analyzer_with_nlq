import os
from openai import OpenAI
from dotenv import load_dotenv
import json
import logging
import re

load_dotenv()
logger = logging.getLogger(__name__)

SCHEMA_DEFINITION = """
## DATABASE SCHEMA:

Table: revenue_tracker (PostgreSQL/Supabase)
Columns:
- upload_id (TEXT): Unique identifier - REQUIRED IN ALL QUERIES
- unit, product, region, country, customer, category, project_code (TEXT): Dimensions
- year (INTEGER): Year (2025)
- month (TEXT): JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
- budget (NUMERIC): Monthly budgeted amount ✅ VALID FOR QUERIES
- projected (NUMERIC): Monthly projected amount ✅ VALID FOR QUERIES
- actual (NUMERIC): Monthly actual amount ✅ VALID FOR QUERIES
- total, ytd_actual, remaining_projection (NUMERIC): Summary columns - DO NOT USE IN SUM!

## CRITICAL RULES:

### Rule 1: Table Name & upload_id
- Table: revenue_tracker
- MUST include: WHERE upload_id = '{upload_id}'

### Rule 2: Valid Metrics
✅ CORRECT: SUM(actual), SUM(projected), SUM(budget)
❌ FORBIDDEN: SUM(total), SUM(ytd_actual), SUM(remaining_projection)

### Rule 3: Year Handling
When NO year specified:
```sql
SELECT year, month, SUM(actual) as revenue
FROM revenue_tracker
WHERE upload_id = '...' AND month = 'MAR'
GROUP BY year, month
ORDER BY year, month
```

When year specified:
```sql
SELECT SUM(actual) as revenue
FROM revenue_tracker  
WHERE upload_id = '...' AND month = 'MAR' AND year = 2025
```

### Rule 4: Return Complete Metadata
EVERY query must return detailed filters:
```json
{
  "metric_used": "actual|projected|budget",
  "filters_applied": {
    "month": "JAN" or null,
    "year": 2025 or null,
    "customer": "ADIB" or null,
    "region": "AFR" or null
  },
  "group_by": ["year", "month"] or [],
  "aggregation": "sum|avg|max|min|count"
}
```
"""

class SQLGenerator:
    def __init__(self):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found")
        self.client = OpenAI(api_key=api_key)
        logger.info("SQL Generator initialized")
    
    def generate_sql(self, user_question: str, upload_id: str, conversation_history=None):
        """Generate SQL with complete metadata"""
        
        logger.info(f"Generating SQL for: {user_question}")
        
        context = ""
        if conversation_history:
            context = "\n## CONVERSATION HISTORY:\n"
            for item in conversation_history[-3:]:
                context += f"Q: {item.get('question', '')}\nA: {item.get('answer', '')}\n\n"
        
        system_prompt = f"""You are a SQL expert for revenue data analysis.

{SCHEMA_DEFINITION}

## YOUR TASK:
1. Generate valid PostgreSQL query
2. Table: revenue_tracker
3. MANDATORY: WHERE upload_id = '{upload_id}'
4. Return COMPLETE metadata about ALL filters used

## METADATA REQUIREMENTS:
You MUST extract and return ALL filters applied:
- metric_used: "actual", "projected", or "budget"
- filters_applied: ALL WHERE conditions (month, year, customer, region, etc.)
- group_by: Any GROUP BY columns (especially year when not specified)
- aggregation: sum, avg, max, min, count, or none

## EXAMPLES:

Query: "What is total budget for 2025?"
Response:
{{
  "can_answer": true,
  "sql": "SELECT SUM(budget) as total FROM revenue_tracker WHERE upload_id = '{upload_id}' AND year = 2025",
  "metadata": {{
    "metric_used": "budget",
    "filters_applied": {{"year": 2025}},
    "group_by": [],
    "aggregation": "sum"
  }}
}}

Query: "Show actual revenue for March"
Response:
{{
  "can_answer": true,
  "sql": "SELECT year, SUM(actual) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND month = 'MAR' GROUP BY year ORDER BY year",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{"month": "MAR"}},
    "group_by": ["year"],
    "aggregation": "sum"
  }}
}}

Query: "Projected revenue for ADIB in Q1"
Response:
{{
  "can_answer": true,
  "sql": "SELECT year, month, SUM(projected) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND customer ILIKE '%adib%' AND month IN ('JAN','FEB','MAR') GROUP BY year, month ORDER BY year, month",
  "metadata": {{
    "metric_used": "projected",
    "filters_applied": {{"customer": "ADIB", "quarter": "Q1", "months": ["JAN","FEB","MAR"]}},
    "group_by": ["year", "month"],
    "aggregation": "sum"
  }}
}}

## CRITICAL:
- NEVER use SUM(total), use SUM(actual/projected/budget)
- Include year in GROUP BY when year not specified in question
- Return ALL filters in metadata

{context}
"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_question}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            logger.info(f"Generated SQL: {result.get('sql', 'N/A')[:100]}")
            logger.info(f"Metadata: {result.get('metadata', {})}")
            
            if result.get('can_answer') and result.get('sql'):
                # Validate query
                validation_errors = self.validate_sql_query(result['sql'], upload_id)
                if validation_errors:
                    logger.error(f"Validation errors: {validation_errors}")
                    return {
                        'can_answer': False,
                        'explanation': f"Query validation failed: {', '.join(validation_errors)}"
                    }
                
                # Extract actual filters from SQL for accuracy
                extracted_filters = self._extract_filters_from_sql(result['sql'])
                
                # Merge with metadata
                if 'filters_applied' not in result['metadata']:
                    result['metadata']['filters_applied'] = {}
                result['metadata']['filters_applied'].update(extracted_filters)
                
                logger.info(f"Final metadata: {result['metadata']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error generating SQL: {str(e)}", exc_info=True)
            return {
                'can_answer': False,
                'explanation': f"Error: {str(e)}"
            }
    
    def _extract_filters_from_sql(self, sql: str) -> dict:
        """Extract actual filters from SQL query for accuracy"""
        filters = {}
        sql_upper = sql.upper()
        
        # Extract month
        month_match = re.search(r"MONTH\s*=\s*'([A-Z]{3})'", sql_upper)
        if month_match:
            filters['month'] = month_match.group(1)
        
        months_match = re.search(r"MONTH\s+IN\s*\('([^']+)'(?:,\s*'([^']+)')*\)", sql_upper)
        if months_match:
            months = re.findall(r"'([A-Z]{3})'", month_match.group(0) if month_match else months_match.group(0))
            filters['months'] = months
        
        # Extract year
        year_match = re.search(r"YEAR\s*=\s*(\d{4})", sql_upper)
        if year_match:
            filters['year'] = int(year_match.group(1))
        
        # Extract customer
        customer_match = re.search(r"CUSTOMER\s+(?:I?LIKE|=)\s*'%?([^'%]+)%?'", sql_upper)
        if customer_match:
            filters['customer'] = customer_match.group(1)
        
        # Extract region
        region_match = re.search(r"REGION\s*=\s*'([^']+)'", sql_upper)
        if region_match:
            filters['region'] = region_match.group(1)
        
        # Extract GROUP BY
        group_match = re.search(r"GROUP\s+BY\s+([^\s]+(?:\s*,\s*[^\s]+)*)", sql_upper)
        if group_match:
            filters['grouped_by'] = [col.strip() for col in group_match.group(1).split(',')]
        
        return filters
    
    def validate_sql_query(self, query: str, upload_id: str):
        """Validate SQL query"""
        errors = []
        query_upper = query.upper()
        query_lower = query.lower()
        
        # Check table name
        if 'revenue_tracker' not in query_lower:
            errors.append("Must use table 'revenue_tracker'")
        
        # Check upload_id
        if 'upload_id' not in query_lower:
            errors.append(f"Must include WHERE upload_id = '{upload_id}'")
        
        # Check forbidden patterns
        forbidden = [
            ('SUM(TOTAL)', 'Use SUM(actual/projected/budget)'),
            ('SUM(YTD_ACTUAL)', 'Use SUM(actual)'),
            ('SUM(REMAINING_PROJECTION)', 'Use SUM(projected)'),
            ('AVG(TOTAL)', 'Use AVG(actual/projected/budget)'),
        ]
        
        query_normalized = query_upper.replace(' ', '')
        for pattern, msg in forbidden:
            if pattern.replace(' ', '') in query_normalized:
                errors.append(msg)
        
        return errors

def generate_sql_query(user_question, upload_id, conversation_history=None):
    """Backward compatibility"""
    generator = SQLGenerator()
    return generator.generate_sql(user_question, upload_id, conversation_history)