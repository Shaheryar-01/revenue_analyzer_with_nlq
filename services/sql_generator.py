import os
from openai import OpenAI
from dotenv import load_dotenv
import json
import logging
import re

load_dotenv()
logger = logging.getLogger(__name__)

class SQLGenerator:
    def __init__(self):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found")
        self.client = OpenAI(api_key=api_key)
        logger.info("SQL Generator initialized")
    



    def _resolve_entity_column(self, value: str, entity_metadata: dict) -> str:
            """
            Determine which column (unit, region, customer, category, etc.)
            a given entity value belongs to, using metadata lists.
            """
            if not entity_metadata or not value:
                return None
            
            val_lower = str(value).strip().lower()
            
            for column, entities in {
                "unit": entity_metadata.get("units", []),
                "region": entity_metadata.get("regions", []),
                "category": entity_metadata.get("categories", []),
                "country": entity_metadata.get("countries", []),
                "customer": entity_metadata.get("customers", []),
                "product": entity_metadata.get("products", []),
                "project_code": entity_metadata.get("project_codes", [])
            }.items():
                if any(val_lower == e.strip().lower() for e in entities):
                    return column
            
            return None  # fallback


    def generate_sql(self, user_question: str, upload_id: str, entity_metadata: dict = None, conversation_history=None):
        """Generate SQL with entity awareness and complete metadata"""
        
        logger.info(f"Generating SQL for: {user_question}")
        if entity_metadata:
            logger.info(f" Using entity metadata: Units={len(entity_metadata.get('units', []))}, Regions={len(entity_metadata.get('regions', []))}")
        
        # Build schema definition with entity metadata
        schema_definition = self._build_schema_with_entities(upload_id, entity_metadata)
        
        context = ""
        if conversation_history:
            context = "\n## CONVERSATION HISTORY:\n"
            for item in conversation_history[-3:]:
                context += f"Q: {item.get('question', '')}\nA: {item.get('answer', '')}\n\n"
        
        system_prompt = f"""{schema_definition}

## YOUR TASK:
1. Generate valid PostgreSQL query
2. Table: revenue_tracker
3. MANDATORY: WHERE upload_id = '{upload_id}'
4. Return COMPLETE metadata about ALL filters used

## METADATA REQUIREMENTS:
You MUST extract and return ALL filters applied:
- metric_used: "actual", "projected", or "budget"
- filters_applied: ALL WHERE conditions (month, year, customer, region, unit, etc.)
- group_by: Any GROUP BY columns (especially year when not specified)
- aggregation: sum, avg, max, min, count, or none

## CRITICAL RULES:


### Rule 0: NULL Handling Strategy (MOST IMPORTANT!)

**For Metric Columns (actual, projected, budget):**
ALWAYS use COALESCE to treat NULL as 0:
❌ WRONG: SUM(actual)
✅ CORRECT: SUM(COALESCE(actual, 0))

❌ WRONG: AVG(budget)
✅ CORRECT: AVG(COALESCE(budget, 0))

**For Grouping Columns (region, unit, customer, category, product):**
ALWAYS exclude NULLs when grouping by that column:
❌ WRONG: WHERE upload_id = '...' GROUP BY region
✅ CORRECT: WHERE upload_id = '...' AND region IS NOT NULL GROUP BY region

❌ WRONG: WHERE upload_id = '...' GROUP BY customer
✅ CORRECT: WHERE upload_id = '...' AND customer IS NOT NULL GROUP BY customer

**For ORDER BY with LIMIT (finding MAX/MIN/TOP/BOTTOM):**
ALWAYS use NULLS LAST to prevent NULL from being picked as "highest":
❌ WRONG: ORDER BY revenue DESC LIMIT 1
✅ CORRECT: ORDER BY revenue DESC NULLS LAST LIMIT 1    

❌ WRONG: ORDER BY revenue ASC LIMIT 5
✅ CORRECT: ORDER BY revenue ASC NULLS LAST LIMIT 5

**Complete Example:**
```sql-- Query: "Which region has highest revenue?"
SELECT region, SUM(COALESCE(actual, 0)) as revenue
FROM revenue_tracker
WHERE upload_id = '{upload_id}'
AND region IS NOT NULL
GROUP BY region
ORDER BY revenue DESC NULLS LAST
LIMIT 1

### Rule 1: Use Entity Metadata for Column Matching
When entity metadata is provided, use it to determine the correct column:
- If entity appears in 'units' list → Use 'unit' column
- If entity appears in 'regions' list → Use 'region' column
- If entity appears in 'customers' list → Use 'customer' column
- If entity appears in 'products' list → Use 'product' column

### Rule 2: Customer Name Matching
For customer filters, ALWAYS use ILIKE for partial matching:
❌ WRONG: WHERE customer = 'ADIB Bank'
❌ WRONG: WHERE customer IN ('ADIB Bank', 'HBL')
✅ CORRECT: WHERE customer ILIKE '%ADIB%'
✅ CORRECT: WHERE customer ILIKE '%ADIB%' OR customer ILIKE '%HBL%'

### Rule 3: Year Handling
When NO year specified, group by year to show breakdown:
```sql
SELECT year, month, SUM(actual) as revenue
FROM revenue_tracker
WHERE upload_id = '...' AND month = 'MAR'
GROUP BY year, month
ORDER BY year, month
```

### Rule X: Always Use NULLS LAST in ORDER BY
When ordering by a metric for MAX/MIN/TOP queries:
❌ WRONG: ORDER BY revenue DESC LIMIT 1
✅ CORRECT: ORDER BY revenue DESC NULLS LAST LIMIT 1

❌ WRONG: ORDER BY revenue ASC LIMIT 1  
✅ CORRECT: ORDER BY revenue ASC NULLS LAST LIMIT 1

This ensures NULL values don't incorrectly appear as "highest" or "lowest".

When year specified, include it in WHERE:
```sql
SELECT SUM(actual) as revenue
FROM revenue_tracker  
WHERE upload_id = '...' AND month = 'MAR' AND year = 2025
```

## EXAMPLES:

Example 1: "What is total budget for 2025?"
```json
{{
  "can_answer": true,
  "sql": "SELECT SUM(COALESCE(budget, 0)) as total FROM revenue_tracker WHERE upload_id = '{upload_id}' AND year = 2025",
  "metadata": {{
    "metric_used": "budget",
    "filters_applied": {{"year": 2025}},
    "group_by": [],
    "aggregation": "sum"
  }}
}}
```

Example 2: "Show actual revenue for March" (no year specified)
```json
{{
  "can_answer": true,
  "sql": "SELECT year, SUM(COALESCE(actual, 0)) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND month = 'MAR' GROUP BY year ORDER BY year",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{"month": "MAR"}},
    "group_by": ["year"],
    "aggregation": "sum"
  }}
}}
```

Example 3: "Projected revenue for ADIB in Q1"
```json
{{
  "can_answer": true,
  "sql": "SELECT year, month, SUM(COALESCE(projected, 0)) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND customer ILIKE '%ADIB%' AND month IN ('JAN','FEB','MAR') GROUP BY year, month ORDER BY year, month",
  "metadata": {{
    "metric_used": "projected",
    "filters_applied": {{"customer": "ADIB", "quarter": "Q1", "months": ["JAN","FEB","MAR"]}},
    "group_by": ["year", "month"],
    "aggregation": "sum"
  }}
}}
```

Example 4: "Compare AMS vs CRM" (using entity metadata - both are Units)
```json
{{
  "can_answer": true,
  "sql": "SELECT unit, SUM(COALESCE(actual, 0)) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND unit IN ('AMS', 'CRM') AND unit IS NOT NULL GROUP BY unit ORDER BY revenue DESC NULLS LAST",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{"unit": ["AMS", "CRM"]}},
    "group_by": ["unit"],
    "aggregation": "sum"
  }}
}}
```

Example 5: "Which customer had biggest increase vs last year?"
```json
{{
  "can_answer": true,
  "sql": "WITH yearly_revenue AS (SELECT customer, year, SUM(COALESCE(actual, 0)) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND customer IS NOT NULL GROUP BY customer, year) SELECT customer, revenue - LAG(revenue) OVER (PARTITION BY customer ORDER BY year) as growth FROM yearly_revenue ORDER BY growth DESC NULLS LAST LIMIT 1",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{}},
    "group_by": ["customer", "year"],
    "aggregation": "sum"
  }}
}}
```

Example 6: "List customers with revenue over $100k"
```json
{{
  "can_answer": true,
  "sql": "SELECT customer, SUM(COALESCE(actual, 0)) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND customer IS NOT NULL GROUP BY customer HAVING SUM(COALESCE(actual, 0)) > 100000 ORDER BY revenue DESC NULLS LAST",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{}},
    "group_by": ["customer"],
    "aggregation": "sum"
  }}
}}
```

Example 7: "What percentage of annual target have we achieved?"
```json
{{
  "can_answer": true,
  "sql": "SELECT year, (SUM(COALESCE(actual, 0)) / NULLIF(SUM(COALESCE(budget, 0)), 0) * 100) as achievement_pct FROM revenue_tracker WHERE upload_id = '{upload_id}' GROUP BY year ORDER BY year",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{}},
    "group_by": ["year"],
    "aggregation": "none"
  }}
}}
```

Example 8: "Which region has highest revenue?"
```json
{{
  "can_answer": true,
  "sql": "SELECT region, SUM(COALESCE(actual, 0)) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND region IS NOT NULL GROUP BY region ORDER BY revenue DESC NULLS LAST LIMIT 1",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{}},
    "group_by": ["region"],
    "aggregation": "sum"
  }}
}}
```

## YEAR-OVER-YEAR QUERIES - ALWAYS USE CTE:

For YoY comparisons, ALWAYS use this CTE pattern:

```sql
WITH yearly_revenue AS (
    SELECT customer, year, SUM(actual) as revenue
    FROM revenue_tracker
    WHERE upload_id = '{upload_id}'
    GROUP BY customer, year
)
SELECT customer,
       revenue - LAG(revenue) OVER (PARTITION BY customer ORDER BY year) as growth
FROM yearly_revenue
ORDER BY growth DESC NULLS LAST
LIMIT 1
```

## FORBIDDEN:
- ❌ NEVER use SUM(total), use SUM(actual/projected/budget)
- ❌ NEVER use SUM(ytd_actual), use SUM(actual)
- ❌ NEVER use customer IN (...) for exact match, use ILIKE
- ❌ NEVER use LAG() with GROUP BY without CTE

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
                
                logger.info(f" Final metadata: {result['metadata']}")

                                # --- AUTO DISAMBIGUATION PATCH ---
                try:
                    filters_applied = result["metadata"].get("filters_applied", {})
                    if filters_applied and entity_metadata:
                        corrected_filters = {}
                        for key, val in list(filters_applied.items()):
                            if isinstance(val, list):
                                # Handle multi-value filters
                                corrected_list = []
                                for v in val:
                                    correct_col = self._resolve_entity_column(v, entity_metadata)
                                    if correct_col:
                                        corrected_list.append((correct_col, v))
                                for col, v in corrected_list:
                                    corrected_filters.setdefault(col, []).append(v)
                            else:
                                correct_col = self._resolve_entity_column(val, entity_metadata)
                                if correct_col and correct_col != key:
                                    logger.info(f"[AUTO FIX] Reassigned filter: {key} → {correct_col} for '{val}'")
                                    corrected_filters.setdefault(correct_col, val)
                                else:
                                    corrected_filters.setdefault(key, val)

                        # Apply the corrected filters
                        result["metadata"]["filters_applied"] = corrected_filters

                except Exception as e:
                    logger.warning(f"[AUTO FIX] Filter disambiguation failed: {e}")
                # ---------------------------------

                    
            
            return result
            
        except Exception as e:
            logger.error(f"Error generating SQL: {str(e)}", exc_info=True)
            return {
                'can_answer': False,
                'explanation': f"Error: {str(e)}"
            }
    
    def _build_schema_with_entities(self, upload_id: str, entity_metadata: dict = None) -> str:
        """Build schema definition with entity metadata for better accuracy"""
        
        base_schema = """## DATABASE SCHEMA:

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
"""
        
        if entity_metadata:
            entities_section = "\n## AVAILABLE ENTITIES IN THIS UPLOAD:\n\n"
            
            if entity_metadata.get('units'):
                units = ', '.join(entity_metadata['units'][:20])
                entities_section += f"**Units (use 'unit' column):** {units}\n"
            
            if entity_metadata.get('regions'):
                regions = ', '.join(entity_metadata['regions'])
                entities_section += f"**Regions (use 'region' column):** {regions}\n"
            
            if entity_metadata.get('categories'):
                categories = ', '.join(entity_metadata['categories'])
                entities_section += f"**Categories (use 'category' column):** {categories}\n"
            
            if entity_metadata.get('countries'):
                countries = ', '.join(entity_metadata['countries'])
                entities_section += f"**Countries (use 'country' column):** {countries}\n"
            
            if entity_metadata.get('customers'):
                customer_count = len(entity_metadata['customers'])
                customer_sample = ', '.join([c[:30] + '...' if len(c) > 30 else c for c in entity_metadata['customers'][:5]])
                entities_section += f"**Customers ({customer_count} total, use 'customer' column with ILIKE):** {customer_sample}, ...\n"
            
            if entity_metadata.get('products'):
                product_count = len(entity_metadata['products'])
                product_sample = ', '.join([p[:40] + '...' if len(p) > 40 else p for p in entity_metadata['products'][:5]])
                entities_section += f"**Products ({product_count} total, use 'product' column):** {product_sample}, ...\n"
            
            if entity_metadata.get('project_codes'):
                project_code_count = len(entity_metadata['project_codes'])
                project_code_sample = ', '.join(entity_metadata['project_codes'][:10])
                entities_section += f"**Project Codes ({project_code_count} total, use 'project_code' column):** {project_code_sample}, ...\n"
                        
            entities_section += """
## ENTITY RECOGNITION RULES:

**CRITICAL: ALWAYS check entity metadata FIRST before guessing!**

### Step-by-step process:
1. User mentions: "funnel"
2. Check if "funnel" (case-insensitive) exists in:
   - Units list? NO
   - Categories list? YES → Use WHERE category ILIKE '%funnel%'
   - Customers list? Don't check further
   
3. User mentions: "new sales"
   - Units? NO
   - Categories? YES → Use WHERE category ILIKE '%new sales%'
   
4. User mentions: "ADIB"
   - Units? NO
   - Categories? NO
   - Customers? YES → Use WHERE customer ILIKE '%ADIB%'

### Priority Order:
1. Check Units first (most specific)
2. Check Regions second
3. Check Categories third
4. Check Countries fourth
5. Check Customers fifth (use ILIKE)
6. Check Products sixth
7. Check Project Codes seventh

### Common Mistakes to AVOID:
❌ User says "funnel" → You assume it's a customer → WRONG!
✅ User says "funnel" → Check categories list → Found! → WHERE category ILIKE '%funnel%'

❌ User says "new sales" → You assume it's a product → WRONG!
✅ User says "new sales" → Check categories list → Found! → WHERE category ILIKE '%new sales%'
"""
            return base_schema + entities_section
        
        return base_schema
    
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
            months = re.findall(r"'([A-Z]{3})'", months_match.group(0))
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
        
        # Extract unit
        unit_match = re.search(r"UNIT\s*=\s*'([^']+)'", sql_upper)
        if unit_match:
            filters['unit'] = unit_match.group(1)
        
        # Extract unit IN clause
        unit_in_match = re.search(r"UNIT\s+IN\s*\(([^)]+)\)", sql_upper)
        if unit_in_match:
            units = re.findall(r"'([^']+)'", unit_in_match.group(1))
            filters['unit'] = units
        
        # Extract GROUP BY
        group_match = re.search(r"GROUP\s+BY\s+([^\s]+(?:\s*,\s*[^\s]+)*)", sql_upper)
        if group_match:
            filters['grouped_by'] = [col.strip() for col in group_match.group(1).split(',')]
        


        country_match = re.search(r"COUNTRY\s*=\s*'([^']+)'", sql_upper)
        if country_match:
            filters['country'] = country_match.group(1)
        
        # Extract category
        category_match = re.search(r"CATEGORY\s*=\s*'([^']+)'", sql_upper)
        if category_match:
            filters['category'] = category_match.group(1)


        category_like_match = re.search(r"CATEGORY\s+(?:I?LIKE)\s*'%?([^'%]+)%?'", sql_upper)
        if category_like_match:
            filters['category'] = category_like_match.group(1)
        
        # Extract product
        product_match = re.search(r"PRODUCT\s*=\s*'([^']+)'", sql_upper)
        if product_match:
            filters['product'] = product_match.group(1)
        
        # Extract product with LIKE/ILIKE
        product_like_match = re.search(r"PRODUCT\s+(?:I?LIKE)\s*'%?([^'%]+)%?'", sql_upper)
        if product_like_match:
            filters['product'] = product_like_match.group(1)
        
        # Extract project_code
        project_code_match = re.search(r"PROJECT_CODE\s*=\s*'([^']+)'", sql_upper)
        if project_code_match:
            filters['project_code'] = project_code_match.group(1)


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
    return generator.generate_sql(user_question, upload_id, conversation_history=conversation_history)