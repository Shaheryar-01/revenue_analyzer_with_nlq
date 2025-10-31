import os
from openai import OpenAI
from dotenv import load_dotenv
import json
import logging
import re

load_dotenv()
logger = logging.getLogger(__name__)


def normalize_entity_metadata(entity_metadata: dict) -> dict:
    """Normalize all metadata values for case-insensitive comparison"""
    if not entity_metadata:
        return {}
    normalized = {}
    for key, values in entity_metadata.items():
        if isinstance(values, list):
            normalized[key.lower()] = [str(v).strip().lower() for v in values if v]
    return normalized


def resolve_entity_from_metadata(value: str, entity_metadata: dict) -> str:
    """
    Dynamically resolve which entity type a value belongs to.
    Returns one of: unit, region, category, country, customer, product, project_code
    """
    if not entity_metadata or not value:
        return None

    val = str(value).strip().lower()
    for col, entities in entity_metadata.items():
        if isinstance(entities, list):
            for e in entities:
                if str(e).strip().lower() == val:
                    return col  # return the matching entity type
    return None


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
        entity_metadata = normalize_entity_metadata(entity_metadata)

        mapping = {
            "unit": entity_metadata.get("units", []),
            "region": entity_metadata.get("regions", []),
            "category": entity_metadata.get("categories", []),
            "country": entity_metadata.get("countries", []),
            "customer": entity_metadata.get("customers", []),
            "product": entity_metadata.get("products", []),
            "project_code": entity_metadata.get("project_codes", [])
        }

        for col, entities in mapping.items():
            if val_lower in entities:
                return col
        return None


    def generate_sql(
        self,
        user_question: str,
        upload_id: str,
        entity_metadata: dict = None,
        resolved_entities: dict = None,
        conversation_history=None
    ):
        """Generate SQL with entity awareness, comparison mode, and full metadata."""

        import json
        import re

        logger.info(f"Generating SQL for: {user_question}")

        # Normalize entity metadata (standardizes casing / keys)
        if entity_metadata:
            logger.info(f"Using entity metadata: Units={len(entity_metadata.get('units', []))}, Regions={len(entity_metadata.get('regions', []))}")
            entity_metadata = normalize_entity_metadata(entity_metadata)

        # -------------------------------------------
        #  COMPARISON DETECTION (AMS vs CRM, etc.)
        # -------------------------------------------
        compare_keywords = ["compare", "vs", "versus", "relative", "difference", "compare with", "compare to"]
        user_lower = user_question.lower()
        is_comparison = any(k in user_lower for k in compare_keywords)

        if is_comparison and conversation_history:
            logger.info("[COMPARE MODE] Comparison detected. Checking previous filters...")

            last_query = None
            for item in reversed(conversation_history):
                meta = item.get("metadata", {})
                if meta.get("filters_applied"):
                    last_query = meta
                    break

            if last_query:
                previous_filters = last_query.get("filters_applied", {}).copy()
                logger.info(f"[COMPARE MODE] Previous filters detected: {previous_filters}")

                # Extract NEW comparison word (last token or quoted expression)
                match = re.findall(r"'([^']+)'|\"([^\"]+)\"|([A-Za-z0-9_-]+)$", user_question)
                if match:
                    new_value = next(v for group in match for v in group if v).strip()
                    logger.info(f"[COMPARE MODE] New comparison value detected: {new_value}")

                    # Identify correct column using metadata
                    col = self._resolve_entity_column(new_value, entity_metadata)

                    if col:
                        logger.info(f"[COMPARE MODE] Entity '{new_value}' resolved to column: {col}")

                        # Ensure list type
                        existing = previous_filters.get(col, [])
                        if not isinstance(existing, list):
                            existing = [existing] if existing else []

                        if new_value not in existing:
                            existing.append(new_value)

                        previous_filters[col] = existing

                        # Update resolved_entities forcefully
                        if resolved_entities is None:
                            resolved_entities = {}

                        for v in existing:
                            resolved_entities[v] = {"type": col, "confidence": "certain"}

                        last_query["filters_applied"] = previous_filters
                        logger.info(f"[COMPARE MODE] Final comparison filter: {previous_filters[col]}")

        # -------------------------------------------
        # BUILD SCHEMA + CONVERSATION HISTORY CONTEXT
        # -------------------------------------------
        schema_definition = self._build_schema_with_entities(upload_id, entity_metadata)

        context = ""
        if conversation_history:
            context = "\n## CONVERSATION HISTORY:\n"
            for item in conversation_history[-3:]:
                context += f"Q: {item.get('question', '')}\nA: {item.get('answer', '')}\n\n"

        # -------------------------------------------
        # PRE–RESOLVED ENTITY SECTION (FORCING CORRECT COLUMN)
        # -------------------------------------------
        resolved_section = ""
        if resolved_entities:
            resolved_section = "\n## PRE-RESOLVED ENTITIES (USE THESE!):\n\n"
            resolved_section += "These values MUST map to these exact columns:\n\n"
            for value, res in resolved_entities.items():
                resolved_section += f"  - '{value}' → **{res['type']}** (confidence: {res['confidence']})\n"
            resolved_section += "\nAlways use ILIKE '%value%' for text-based entity matches.\n\n"

        # -------------------------------------------
        # MASTER SYSTEM PROMPT (DO NOT MODIFY)
        # -------------------------------------------
        system_prompt = (
            schema_definition
            + resolved_section
            + context
            + """
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

** For Metric Columns (actual, projected, budget): **
ALWAYS use COALESCE to treat NULL as 0:
WRONG: SUM(actual)
CORRECT: SUM(COALESCE(actual, 0))

WRONG: AVG(budget)
CORRECT: AVG(COALESCE(budget, 0))

** For Grouping Columns (region, unit, customer, category, product): **
ALWAYS exclude NULLs when grouping by that column:
WRONG: WHERE upload_id = '...' GROUP BY region
CORRECT: WHERE upload_id = '...' AND region IS NOT NULL GROUP BY region

WRONG: WHERE upload_id = '...' GROUP BY customer
CORRECT: WHERE upload_id = '...' AND customer IS NOT NULL GROUP BY customer

** For ORDER BY with LIMIT (finding MAX/MIN/TOP/BOTTOM): **
ALWAYS use NULLS LAST to prevent NULL from being picked as "highest":
WRONG: ORDER BY revenue DESC LIMIT 1
CORRECT: ORDER BY revenue DESC NULLS LAST LIMIT 1    

WRONG: ORDER BY revenue ASC LIMIT 5
CORRECT: ORDER BY revenue ASC NULLS LAST LIMIT 5

** Complete Example: **
```sql
-- Query: "Which region has highest revenue?"
SELECT region, SUM(COALESCE(actual, 0)) as revenue
FROM revenue_tracker
WHERE upload_id = '{upload_id}'
AND region IS NOT NULL
GROUP BY region
ORDER BY revenue DESC NULLS LAST
LIMIT 1
```

### Rule 1: Use Entity Metadata for Column Matching
When entity metadata is provided, use it to determine the correct column:
- If entity appears in 'units' list -> Use 'unit' column
- If entity appears in 'regions' list -> Use 'region' column
- If entity appears in 'customers' list -> Use 'customer' column
- If entity appears in 'products' list -> Use 'product' column

### Rule 2: ILIKE Pattern for Text-Based Entity Filters

** CRITICAL: Use ILIKE for ALL text-based entity columns: **

** TEXT ENTITIES (use ILIKE with % wildcards): **
- unit: WHERE unit ILIKE '%value%'
- product: WHERE product ILIKE '%value%'
- region: WHERE region ILIKE '%value%'
- country: WHERE country ILIKE '%value%'
- customer: WHERE customer ILIKE '%value%'
- category: WHERE category ILIKE '%value%'

** NUMERIC/CODE ENTITIES (use exact match): **
- project_code: WHERE project_code = 'value' (exact match, no ILIKE)

** Examples: **

WRONG: WHERE product = 'novus'
CORRECT: WHERE product ILIKE '%novus%'

WRONG: WHERE unit = 'AMS'
CORRECT: WHERE unit ILIKE '%AMS%'

WRONG: WHERE region = 'ME'
CORRECT: WHERE region ILIKE '%ME%'

WRONG: WHERE customer = 'ADIB Bank'
CORRECT: WHERE customer ILIKE '%ADIB%'

CORRECT: WHERE project_code = '22-06-02-24' (exact match for codes)

### Rule 3: Multiple Values - Use OR with ILIKE

When filtering by multiple values of same entity:

WRONG: WHERE product IN ('novus', 'ambit')
CORRECT: WHERE (product ILIKE '%novus%' OR product ILIKE '%ambit%')

WRONG: WHERE unit IN ('AMS', 'CRM')
CORRECT: WHERE (unit ILIKE '%AMS%' OR unit ILIKE '%CRM%')

### Rule 4: Year Handling
When NO year specified, group by year to show breakdown:
```sql
SELECT year, month, SUM(COALESCE(actual, 0)) as revenue
FROM revenue_tracker
WHERE upload_id = '...' AND month = 'MAR'
GROUP BY year, month
ORDER BY year, month
```

When year specified, include it in WHERE:
```sql
SELECT SUM(COALESCE(actual, 0)) as revenue
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

Example 4A: "Compare AMS vs CRM" (using entity metadata - both are Units)
```json
{{
  "can_answer": true,
  "sql": "SELECT unit, SUM(COALESCE(actual, 0)) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND (unit ILIKE '%AMS%' OR unit ILIKE '%CRM%') AND unit IS NOT NULL GROUP BY unit ORDER BY revenue DESC NULLS LAST",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{"unit": ["AMS", "CRM"]}},
    "group_by": ["unit"],
    "aggregation": "sum"
  }}
}}
```

Example 4B: "Compare Novus vs Rendezvous" (Product Comparison)
```json
{{
  "can_answer": true,
  "sql": "SELECT product, SUM(COALESCE(actual, 0)) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND (product ILIKE '%novus%' OR product ILIKE '%rendezvous%') AND product IS NOT NULL GROUP BY product ORDER BY revenue DESC NULLS LAST",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{"product": ["novus", "rendezvous"]}},
    "group_by": ["product"],
    "aggregation": "sum"
  }}
}}
``` 

Example 5: "Total revenue for Novus product"
```json
{{
  "can_answer": true,
  "sql": "SELECT SUM(COALESCE(actual, 0)) as total_revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND product ILIKE '%novus%'",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{"product": "novus"}},
    "group_by": [],
    "aggregation": "sum"
  }}
}}
``` 

Example 6: "Revenue for Ambit products"
```json
{{
  "can_answer": true,
  "sql": "SELECT SUM(COALESCE(actual, 0)) as total_revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND product ILIKE '%ambit%'",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{"product": "ambit"}},
    "group_by": [],
    "aggregation": "sum"
  }}
}}
```

Example 7: "Which customer had biggest increase vs last year?"
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

Example 8: "List customers with revenue over $100k"
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

Example 9: "What percentage of annual target have we achieved?"
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

Example 10: "Which region has highest revenue?"
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

Example 11: "Revenue for project code 22-06-02-24"
```json
{{
  "can_answer": true,
  "sql": "SELECT SUM(COALESCE(actual, 0)) as total_revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND project_code = '22-06-02-24'",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{"project_code": "22-06-02-24"}},
    "group_by": [],
    "aggregation": "sum"
    }}
}}

Example 12: "Which category consistently exceeded its monthly revenue targets?"
{{
  "can_answer": true,
  "sql": "SELECT category, SUM(COALESCE(actual, 0)) as revenue, SUM(COALESCE(budget, 0)) as target, (SUM(COALESCE(actual, 0)) / NULLIF(SUM(COALESCE(budget, 0)), 0) * 100) as achievement_pct FROM revenue_tracker WHERE upload_id = '{upload_id}' AND category IS NOT NULL GROUP BY category HAVING (SUM(COALESCE(actual, 0)) / NULLIF(SUM(COALESCE(budget, 0)), 0) * 100) > 100 ORDER BY achievement_pct DESC",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{}},
    "group_by": ["category"],
    "aggregation": "sum"
  }}
}}

Example 13: "Which customers achieved over 110% of their projected target?"
{{
  "can_answer": true,
  "sql": "SELECT customer, SUM(COALESCE(projected, 0)) as projected, SUM(COALESCE(actual, 0)) as actual, (SUM(COALESCE(actual, 0)) / NULLIF(SUM(COALESCE(projected, 0)), 0) * 100) as achievement_pct FROM revenue_tracker WHERE upload_id = '{upload_id}' AND customer IS NOT NULL GROUP BY customer HAVING (SUM(COALESCE(actual, 0)) / NULLIF(SUM(COALESCE(projected, 0)), 0) * 100) > 110 ORDER BY achievement_pct DESC",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{}},
    "group_by": ["customer"],
    "aggregation": "sum"
  }}
}}
Example 14: "Which unit has the largest positive variance percentage?"
{{
  "can_answer": true,
  "sql": "SELECT unit, SUM(COALESCE(actual, 0)) as actual, SUM(COALESCE(projected, 0)) as projected, ((SUM(COALESCE(actual, 0)) - SUM(COALESCE(projected, 0))) / NULLIF(SUM(COALESCE(projected, 0)), 0) * 100) as variance_percentage FROM revenue_tracker WHERE upload_id = '{upload_id}' AND unit IS NOT NULL GROUP BY unit ORDER BY variance_percentage DESC NULLS LAST LIMIT 1",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{}},
    "group_by": ["unit"],
    "aggregation": "sum"
  }}
}}

Example 15: "Which customers contributed between $50,000 and $150,000 this year?"
{{
  "can_answer": true,
  "sql": "SELECT customer, SUM(COALESCE(actual, 0)) as revenue FROM revenue_tracker WHERE upload_id = '{upload_id}' AND customer IS NOT NULL AND year = 2025 GROUP BY customer HAVING SUM(COALESCE(actual, 0)) BETWEEN 50000 AND 150000 ORDER BY revenue DESC",
  "metadata": {{
    "metric_used": "actual",
    "filters_applied": {{"year": 2025}},
    "group_by": ["customer"],
    "aggregation": "sum"
  }}
}}
```
### Rule 5: Comparison Mode (Multi-Value Filtering)

If `filters_applied` contains a list of values for the same entity (e.g., product = ["novus", "rendezvous"]):

YOU MUST:
1. Use OR with ILIKE for text columns
2. GROUP BY that entity
3. Return the results together in a comparison format.

Example:
WHERE (product ILIKE '%novus%' OR product ILIKE '%rendezvous%')
AND product IS NOT NULL
GROUP BY product
ORDER BY revenue DESC NULLS LAST;

Metadata MUST store multiple values:
"filters_applied": { "product": ["novus", "rendezvous"] }
"group_by": ["product"]
"aggregation": "sum"


## YEAR-OVER-YEAR QUERIES - ALWAYS USE CTE:

For YoY comparisons, ALWAYS use this CTE pattern:

```sql
WITH yearly_revenue AS (
    SELECT customer, year, SUM(COALESCE(actual, 0)) as revenue
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
- NEVER use SUM(total), use SUM(actual/projected/budget)
- NEVER use SUM(ytd_actual), use SUM(actual)
- NEVER use = or IN for text entities (unit, product, region, country, customer, category)
- ALWAYS use ILIKE '%value%' for text entities
- ONLY use = for project_code (numeric codes)
- NEVER use LAG() with GROUP BY without CTE

"""
        ).replace("{upload_id}", upload_id)

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
            logger.info(f"Generated SQL: {result.get('sql', '')[:120]}")

            if not (result.get("can_answer") and result.get("sql")):
                return result

            # Validate SQL
            validation_errors = self.validate_sql_query(result['sql'], upload_id)
            if validation_errors:
                return {
                    'can_answer': False,
                    'explanation': f"Query validation failed: {', '.join(validation_errors)}"
                }

            # Extract filters back from SQL to ensure accuracy
            extracted_filters = self._extract_filters_from_sql(result['sql'])
            result["metadata"].setdefault("filters_applied", {})
            result["metadata"]["filters_applied"].update(extracted_filters)

            # Auto-fix incorrect entity column assignment
            if entity_metadata:
                corrected = {}
                for key, value in result["metadata"]["filters_applied"].items():
                    if isinstance(value, list):
                        for item in value:
                            col = self._resolve_entity_column(item, entity_metadata)
                            if col:
                                corrected.setdefault(col, []).append(item)
                            else:
                                corrected.setdefault(key, []).append(item)
                    else:
                        col = self._resolve_entity_column(value, entity_metadata)
                        if col:
                            corrected[col] = value
                        else:
                            corrected[key] = value

                result["metadata"]["filters_applied"] = corrected

            return result

        except Exception as e:
            logger.error(f"SQL Generation Failure: {e}", exc_info=True)
            return {
                "can_answer": False, 
                "explanation": str(e)
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
- budget (NUMERIC): Monthly budgeted amount - VALID FOR QUERIES
- projected (NUMERIC): Monthly projected amount - VALID FOR QUERIES
- actual (NUMERIC): Monthly actual amount - VALID FOR QUERIES
- total, ytd_actual, remaining_projection (NUMERIC): Summary columns - DO NOT USE IN SUM!
"""
        
        if entity_metadata:
            entities_section = "\n## AVAILABLE ENTITIES IN THIS UPLOAD:\n\n"
            
            if entity_metadata.get('units'):
                units = ', '.join(entity_metadata['units'][:20])
                entities_section += f"** Units (use 'unit' column with ILIKE): ** {units}\n"
            
            if entity_metadata.get('regions'):
                regions = ', '.join(entity_metadata['regions'])
                entities_section += f"** Regions (use 'region' column with ILIKE): ** {regions}\n"
            
            if entity_metadata.get('categories'):
                categories = ', '.join(entity_metadata['categories'])
                entities_section += f"** Categories (use 'category' column with ILIKE): ** {categories}\n"
            
            if entity_metadata.get('countries'):
                countries = ', '.join(entity_metadata['countries'])
                entities_section += f"** Countries (use 'country' column with ILIKE): ** {countries}\n"
            
            if entity_metadata.get('customers'):
                customer_count = len(entity_metadata['customers'])
                customer_sample = ', '.join([c[:30] + '...' if len(c) > 30 else c for c in entity_metadata['customers'][:5]])
                entities_section += f"** Customers ({customer_count} total, use 'customer' column with ILIKE): ** {customer_sample}, ...\n"
            
            if entity_metadata.get('products'):
                product_count = len(entity_metadata['products'])
                product_sample = ', '.join([p[:40] + '...' if len(p) > 40 else p for p in entity_metadata['products'][:5]])
                entities_section += f"** Products ({product_count} total, use 'product' column with ILIKE): ** {product_sample}, ...\n"
            
            if entity_metadata.get('project_codes'):
                project_code_count = len(entity_metadata['project_codes'])
                project_code_sample = ', '.join(entity_metadata['project_codes'][:10])
                entities_section += f"** Project Codes ({project_code_count} total, use 'project_code' column with = exact match): ** {project_code_sample}, ...\n"
                        
            entities_section += """
## ENTITY RECOGNITION RULES:

** CRITICAL: ALWAYS check entity metadata FIRST before guessing! **

### Step-by-step process:
1. User mentions: "funnel"
2. Check if "funnel" (case-insensitive) exists in:
   - Units list? NO
   - Categories list? YES -> Use WHERE category ILIKE '%funnel%'
   - Customers list? Don't check further
   
3. User mentions: "new sales"
   - Units? NO
   - Categories? YES -> Use WHERE category ILIKE '%new sales%'
   
4. User mentions: "ADIB"
   - Units? NO
   - Categories? NO
   - Customers? YES -> Use WHERE customer ILIKE '%ADIB%'

5. User mentions: "novus"
   - Products? YES -> Use WHERE product ILIKE '%novus%'

6. User mentions: "AMS"
   - Units? YES -> Use WHERE unit ILIKE '%AMS%'

### Priority Order:
1. Check Units first (most specific) -> ILIKE
2. Check Regions second -> ILIKE
3. Check Categories third -> ILIKE
4. Check Countries fourth -> ILIKE
5. Check Customers fifth -> ILIKE
6. Check Products sixth -> ILIKE
7. Check Project Codes seventh -> = (exact match)

### Common Mistakes to AVOID:
WRONG: User says "funnel" -> You assume it's a customer -> WRONG!
CORRECT: User says "funnel" -> Check categories list -> Found! -> WHERE category ILIKE '%funnel%'

WRONG: User says "new sales" -> You assume it's a product -> WRONG!
CORRECT: User says "new sales" -> Check categories list -> Found! -> WHERE category ILIKE '%new sales%'

WRONG: User says "novus" -> WHERE product = 'novus' -> WRONG!
CORRECT: User says "novus" -> WHERE product ILIKE '%novus%'
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
        
        # Extract customer (ILIKE pattern)
        customer_match = re.search(r"CUSTOMER\s+ILIKE\s*'%([^'%]+)%'", sql_upper)
        if customer_match:
            filters['customer'] = customer_match.group(1)
        
        # Extract region (ILIKE pattern)
        region_match = re.search(r"REGION\s+ILIKE\s*'%([^'%]+)%'", sql_upper)
        if region_match:
            filters['region'] = region_match.group(1)
        
        # Extract unit (ILIKE pattern)
        unit_match = re.search(r"UNIT\s+ILIKE\s*'%([^'%]+)%'", sql_upper)
        if unit_match:
            filters['unit'] = unit_match.group(1)
        
        # Extract multiple units with OR
        unit_or_matches = re.findall(r"UNIT\s+ILIKE\s*'%([^'%]+)%'", sql_upper)
        if len(unit_or_matches) > 1:
            filters['unit'] = unit_or_matches
        
        # Extract country (ILIKE pattern)
        country_match = re.search(r"COUNTRY\s+ILIKE\s*'%([^'%]+)%'", sql_upper)
        if country_match:
            filters['country'] = country_match.group(1)
        
        # Extract category (ILIKE pattern)
        category_match = re.search(r"CATEGORY\s+ILIKE\s*'%([^'%]+)%'", sql_upper)
        if category_match:
            filters['category'] = category_match.group(1)
        
        # Extract product (ILIKE pattern)
        product_match = re.search(r"PRODUCT\s+ILIKE\s*'%([^'%]+)%'", sql_upper)
        if product_match:
            filters['product'] = product_match.group(1)
        
        # Extract project_code (exact match)
        project_code_match = re.search(r"PROJECT_CODE\s*=\s*'([^']+)'", sql_upper)
        if project_code_match:
            filters['project_code'] = project_code_match.group(1)
        
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
    return generator.generate_sql(user_question, upload_id, conversation_history=conversation_history)