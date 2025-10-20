# app/services/sql_generator.py
from openai import OpenAI
from config.settings import get_settings
import logging
import json

logger = logging.getLogger(__name__)

settings = get_settings()

class SQLGenerator:
    """AI agent that generates SQL queries from natural language"""
    
    def __init__(self):
        try:
            self.client = OpenAI(api_key=settings.openai_api_key)
            logger.info("SQL Generator initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize SQL Generator: {str(e)}")
            raise
    
    def generate_sql(self, user_query: str, upload_id: str) -> dict:
        """
        Generate SQL query from natural language
        
        Args:
            user_query: User's natural language question
            upload_id: Upload identifier to filter data
            
        Returns:
            Dictionary with SQL query and metadata
        """
        logger.info(f"Generating SQL for query: {user_query}")
        
        try:
            prompt = self._build_prompt(user_query, upload_id)
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=1000
            )
            
            raw_response = response.choices[0].message.content
            logger.info(f"Received SQL generation response")
            
            # Parse response
            result = self._parse_response(raw_response)
            
            logger.info(f"SQL generation result: can_answer={result.get('can_answer')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to generate SQL: {str(e)}", exc_info=True)
            return {
                'can_answer': False,
                'sql': None,
                'explanation': f'Error generating SQL: {str(e)}'
            }
    
    def _build_prompt(self, user_query: str, upload_id: str) -> str:
        """Build the prompt for SQL generation"""
        
        prompt = f"""
You are a PostgreSQL expert. Generate SQL queries for a revenue tracking database.

DATABASE SCHEMA:
Table: revenue_tracker

Columns:
- id (UUID): Primary key
- upload_id (TEXT): Filter to identify uploaded file data
- unit (TEXT): Business unit (e.g., 'AMS', 'ME Support', 'Sales Force', 'BSD')
- product (TEXT): Product name
- region (TEXT): Geographic region (e.g., 'ME', 'AFR', 'PK')
- country (TEXT): Country name
- customer (TEXT): Customer name
- category (TEXT): Project category (e.g., 'Funnel', 'New Sales', 'WIH')
- project_code (TEXT): Project identifier
- month (TEXT): Month code ('JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC')
- year (INTEGER): Year (2025)
- budget (DECIMAL): Budget amount
- projected (DECIMAL): Projected revenue
- actual (DECIMAL): Actual revenue
- ytd_actual (DECIMAL): Year-to-date actual
- remaining_projection (DECIMAL): Remaining projection
- total (DECIMAL): Total
- wih_2024, advance_2025, wih_2025, on_hold, wih_2026, shelved (DECIMAL): Various tracking metrics
- created_at (TIMESTAMP): Record creation time

IMPORTANT NOTES:
1. ALWAYS include WHERE upload_id = '{upload_id}' in every query
2. Month values are TEXT: 'JAN', 'FEB', 'MAR', etc.
3. For quarters:
   - Q1 = 'JAN', 'FEB', 'MAR'
   - Q2 = 'APR', 'MAY', 'JUN'
   - Q3 = 'JUL', 'AUG', 'SEP'
   - Q4 = 'OCT', 'NOV', 'DEC'
4. Use COALESCE for handling NULLs in aggregations
5. For "total X" queries, use SUM()
6. For "average X" queries, use AVG()
7. For "which X has highest Y", use ORDER BY Y DESC LIMIT 1
8. ALWAYS use case-insensitive matching: LOWER(column) = LOWER('value')

USER QUERY: "{user_query}"

TASK:
1. Determine if this query can be answered with the schema
2. If yes, generate a PostgreSQL query
3. If no, explain what data is missing

QUERY PATTERNS:

Example 1: "Total actual revenue for AMS"
SQL:
SELECT COALESCE(SUM(actual), 0) as total_revenue
FROM revenue_tracker
WHERE upload_id = '{upload_id}'
  AND LOWER(unit) = 'ams'

Example 2: "Budget for ME Support in Q1"
SQL:
SELECT COALESCE(SUM(budget), 0) as total_budget
FROM revenue_tracker
WHERE upload_id = '{upload_id}'
  AND LOWER(unit) LIKE '%me support%'
  AND month IN ('JAN', 'FEB', 'MAR')

Example 3: "Which customer has highest actual revenue?"
SQL:
SELECT customer, COALESCE(SUM(actual), 0) as total_revenue
FROM revenue_tracker
WHERE upload_id = '{upload_id}'
  AND customer IS NOT NULL
GROUP BY customer
ORDER BY total_revenue DESC
LIMIT 1

Example 4: "Total revenue by region"
SQL:
SELECT region, COALESCE(SUM(actual), 0) as total_revenue
FROM revenue_tracker
WHERE upload_id = '{upload_id}'
  AND region IS NOT NULL
GROUP BY region
ORDER BY total_revenue DESC

Example 5: "Average monthly budget for BSD"
SQL:
SELECT AVG(monthly_budget) as avg_budget
FROM (
    SELECT month, SUM(budget) as monthly_budget
    FROM revenue_tracker
    WHERE upload_id = '{upload_id}'
      AND LOWER(unit) = 'bsd'
    GROUP BY month
) as monthly_totals

Return ONLY valid JSON:
{{
    "can_answer": true/false,
    "sql": "SELECT ... SQL query here" or null,
    "explanation": "reason if cannot answer",
    "query_type": "aggregation" | "ranking" | "comparison" | "simple"
}}
"""
        
        return prompt
    
    def _parse_response(self, raw_response: str) -> dict:
        """Parse AI response"""
        try:
            # Remove markdown code blocks if present
            if "```json" in raw_response:
                raw_response = raw_response.split("```json")[1].split("```")[0]
            elif "```" in raw_response:
                raw_response = raw_response.split("```")[1].split("```")[0]
            
            result = json.loads(raw_response.strip())
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse response as JSON: {str(e)}")
            return {
                'can_answer': False,
                'sql': None,
                'explanation': 'Failed to parse response'
            }