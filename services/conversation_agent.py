# app/services/conversation_agent.py
from openai import OpenAI
import json
from config.settings import get_settings
from typing import Dict, Any, List
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Clear proxy environment variables
proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']
for var in proxy_vars:
    if var in os.environ:
        logger.info(f"Removing proxy environment variable: {var}")
        del os.environ[var]

settings = get_settings()
logger.info("Conversation Agent initializing...")

class ConversationAgent:
    """AI agent for natural conversation and CEO-level insights generation"""
    
    def __init__(self):
        try:
            self.client = OpenAI(api_key=settings.openai_api_key)
            self.conversation_history = {}  # Store by upload_id
            logger.info("Conversation Agent initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Conversation Agent: {str(e)}")
            raise

    # -------------------
    # Intent determination - FIXED VERSION
    # -------------------
    def determine_intent(self, upload_id: str, user_message: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Determine if user needs new analysis or just conversation - and resolve contextual references"""
        logger.info(f"Determining intent for upload_id: {upload_id}, message: {user_message}")
        
        try:
            history = self.get_conversation_history(upload_id)
            
            # Get available columns AND sample values for context
            if schema.get('sheet_count', 1) > 1:
                available_columns = schema.get('all_columns', [])
                sheet_info = f"Multi-sheet data with sheets: {schema.get('sheet_names', [])}"
                # For multi-sheet, get from first sheet
                first_sheet = list(schema.get('sheets', {}).values())[0]
                columns_info = first_sheet.get('columns', {})
            else:
                available_columns = schema.get('basic_info', {}).get('column_names', [])
                sheet_info = f"Single sheet with {len(available_columns)} columns"
                columns_info = schema.get('columns', {})
            
            # ✅ SHOW ALL COLUMNS - with smart grouping for patterns
            column_patterns = schema.get('column_patterns', {})
            sequential_groups = column_patterns.get('sequential_groups', [])
            group_base_names = [g['base_name'] for g in sequential_groups]

            column_details = []
            grouped_columns = set()

            # First, show sequential groups as summaries
            for group in sequential_groups:
                base = group['base_name']
                count = group['count']
                meaning = group['likely_meaning']
                first_col = group['first_column']
                last_col = group['last_column']
                
                # Mark these columns as grouped
                for col in group['columns']:
                    grouped_columns.add(col)
                
                # Show summary instead of listing all
                column_details.append(
                    f"- **{base} (Sequential Group)**: {count} columns from '{first_col}' to '{last_col}' ({meaning})"
                )

            # Then show non-grouped columns individually
            for col in available_columns:
                if col in grouped_columns:
                    continue  # Skip, already shown in group
                
                col_info = columns_info.get(col, {})
                sample_values = col_info.get('sample_values', [])
                
                if sample_values:
                    # Key columns get more samples
                    if col.lower() in ['unit', 'product', 'region', 'category', 'customer', 'country']:
                        samples_str = ', '.join([str(v)[:40] for v in sample_values[:25]])  # ✅ 25 samples
                    else:
                        samples_str = ', '.join([str(v)[:40] for v in sample_values[:15]])  # ✅ 15 samples
                    column_details.append(f"- {col}: {samples_str}")
                else:
                    column_details.append(f"- {col}")

            column_context = "\n".join(column_details)
            
            # DEBUG: Log what we're sending to the AI
            logger.info("=" * 80)
            logger.info("COLUMN CONTEXT BEING SENT TO INTENT DETERMINATION:")
            logger.info(column_context)
            logger.info("=" * 80)
            
            # Build context from recent exchanges
            context_str = ""
            if history and len(history) > 0:
                recent = history[-2:] if len(history) >= 2 else history
                context_parts = []
                for exchange in recent:
                    user_q = exchange.get('user_message', '')
                    assistant_a = exchange.get('assistant_response', '')[:300]
                    context_parts.append(f"Q: {user_q}\nA: {assistant_a}")
                context_str = "\n\n".join(context_parts)
            
            # ✅ ADD PATTERN CONTEXT
            pattern_context = ""
            column_patterns = schema.get('column_patterns', {})
            if column_patterns.get('has_patterns'):
                pattern_context = f"""
    🔍 **IMPORTANT: DETECTED PATTERNS IN THIS FILE**

    {column_patterns.get('summary', '')}

    **QUERY TRANSLATION RULES:**
    """
                
                for group in column_patterns.get('sequential_groups', []):
                    if group['count'] == 12:
                        pattern_context += f"""
    - For {group['base_name']} columns (12 months detected):
    * "January" / "Month 1" / "first month" → {group['columns'][0]}
    * "February" / "Month 2" → {group['columns'][1] if len(group['columns']) > 1 else 'N/A'}
    * "March" / "Month 3" → {group['columns'][2] if len(group['columns']) > 2 else 'N/A'}
    * "April" / "Month 4" → {group['columns'][3] if len(group['columns']) > 3 else 'N/A'}
    * ... up to "December" / "Month 12" → {group['columns'][-1]}
    * "Q1" / "first quarter" → Sum columns: {', '.join(group['columns'][:3])}
    * "Q2" / "second quarter" → Sum columns: {', '.join(group['columns'][3:6])}
    * "Q3" / "third quarter" → Sum columns: {', '.join(group['columns'][6:9])}
    * "Q4" / "fourth quarter" → Sum columns: {', '.join(group['columns'][9:12])}
    * "first half" / "H1" → Sum columns: {', '.join(group['columns'][:6])}
    * "second half" / "H2" → Sum columns: {', '.join(group['columns'][6:12])}
    * "full year" / "annual" → Sum ALL {group['count']} columns: {', '.join(group['columns'])}
    """
                    elif group['count'] == 4:
                        pattern_context += f"""
    - For {group['base_name']} columns (quarterly):
    * "Q1" / "first quarter" → {group['columns'][0]}
    * "Q2" / "second quarter" → {group['columns'][1]}
    * "Q3" / "third quarter" → {group['columns'][2]}
    * "Q4" / "fourth quarter" → {group['columns'][3]}
    * "full year" / "annual" → Sum ALL 4 columns: {', '.join(group['columns'])}
    """
                
                explicit_periods = column_patterns.get('explicit_periods', {})
                if explicit_periods:
                    pattern_context += f"\n**EXPLICIT PERIOD COLUMNS FOUND:**\n"
                    for period_type, cols in explicit_periods.items():
                        pattern_context += f"- {period_type.title()}: {', '.join(cols[:10])}\n"
                
                pattern_context += """

    **CRITICAL:** Queries mentioning months, quarters, or time periods are IN SCOPE. 
    Translate them using the patterns above.
    """
            
            # ✅ ADD FILENAME/YEAR CONTEXT
            filename_context = ""
            inferred_year = column_patterns.get('inferred_year')
            if inferred_year:
                filename_context = f"""

    📅 **CRITICAL FILE CONTEXT:**
    This file contains data for year **{inferred_year}** (detected from filename).

    **IMPORTANT RULES:**
    - When user asks about "{inferred_year}" data, they mean ALL the data in this file
    - Example: "total {inferred_year} projected revenue" = sum ALL Projected columns (all 12 months)
    - Example: "what's our {inferred_year} budget" = sum ALL Budget columns (all 12 months)
    - Example: "{inferred_year} Q1 revenue" = sum first 3 Actual columns
    - Example: "{inferred_year} performance" = analyze all columns in the file
    - The file IS the {inferred_year} dataset - don't mark these queries as OUT_OF_SCOPE

    All columns in this file represent {inferred_year} data unless explicitly stated otherwise.
    """
            
            prompt = f"""
    You are analyzing a user's message in a data analysis context.

    {filename_context}

    {pattern_context}

    DATASET INFO: {sheet_info}

    CRITICAL: Here are the ACTUAL columns and their sample values from the dataset:

    {column_context}

    IMPORTANT COLUMN MAPPING RULES:
    1. "Unit" column contains: Business units/departments like "ME Support", "AMS", "Sales Force", etc.
    2. "Product" column contains: Product names and services
    3. "Region" column contains: Geographic regions (ME, AFR, PK)
    4. "Category" column contains: Project categories (Funnel, New Sales, WIH)
    5. Budget columns contain: Numeric budget values

    When user asks about "ME Support" or any business unit, they are referring to the **Unit** column, NOT Category.

    RECENT CONVERSATION:
    {context_str if context_str else "No previous conversation"}

    NEW USER MESSAGE: "{user_message}"

    CRITICAL SCOPE RULES:
    1. Check if the query mentions VALUES that appear in the sample data above
    2. Check if the query mentions COLUMN NAMES that exist in the list
    3. If query mentions values/entities in the sample data OR column names, it's IN SCOPE
    4. General knowledge questions (e.g., "who is donald trump", "what is python") are OUT OF SCOPE
    5. Questions about data/columns NOT in the available list are OUT OF SCOPE

    YOUR TASKS:
    1. First check: Is this query IN SCOPE or OUT OF SCOPE?
    - IN SCOPE: Mentions columns or values visible in the sample data
    - OUT OF SCOPE: Requires data not present OR general knowledge

    2. If IN SCOPE, determine intent:
    - NEEDS_ANALYSIS: Requires data calculations/aggregations
    - CONVERSATIONAL: Asks about dataset structure, explanations, clarifications

    3. Replace contextual references with actual values from recent conversation:
    - "this", "that", "these" → Replace with actual names from conversation

    4. IDENTIFY THE CORRECT COLUMN:
    - If user mentions a business unit name (like "ME Support", "AMS", "Sales Force"), the query should filter by **Unit** column
    - If user mentions a region (ME, AFR, PK), filter by **Region** column
    - If user mentions a project category (Funnel, WIH, New Sales), filter by **Category** column

    EXAMPLES OF CORRECT COLUMN IDENTIFICATION:

    Example 1:
    User: "budget for ME Support"
    → "ME Support" is in Unit column samples
    → {{"intent": "NEEDS_ANALYSIS", "analysis_query": "total budget where Unit is ME Support"}}

    Example 2:
    User: "budget for ME region"
    → "ME" is in Region column samples
    → {{"intent": "NEEDS_ANALYSIS", "analysis_query": "total budget where Region is ME"}}

    Example 3:
    User: "budget for AMS"
    → "AMS" is in Unit column samples
    → {{"intent": "NEEDS_ANALYSIS", "analysis_query": "total budget where Unit is AMS"}}

    Example 4:
    User: "budget for New Sales"
    → "New Sales" is in Category column samples
    → {{"intent": "NEEDS_ANALYSIS", "analysis_query": "total budget where Category is New Sales"}}

    Example 5:
    User: "What's the total projected revenue for 2025?"
    → File is for 2025, has Projected columns
    → {{"intent": "NEEDS_ANALYSIS", "analysis_query": "sum all Projected columns (12 months of 2025 data)"}}

    Return ONLY valid JSON:
    {{"intent": "NEEDS_ANALYSIS" | "CONVERSATIONAL" | "OUT_OF_SCOPE", "analysis_query": "resolved query with correct column", "reason": "if OUT_OF_SCOPE"}}
    """
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=500
            )
            
            raw_response = response.choices[0].message.content
            logger.info(f"Intent determination raw response: {raw_response[:300]}")
            
            try:
                if "```json" in raw_response:
                    raw_response = raw_response.split("```json")[1].split("```")[0]
                elif "```" in raw_response:
                    raw_response = raw_response.split("```")[1].split("```")[0]
                
                intent_result = json.loads(raw_response.strip())
                
                logger.info(f"Resolved intent: {intent_result['intent']}")
                logger.info(f"Resolved query: {intent_result.get('analysis_query', 'N/A')}")
                
                return intent_result
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse intent JSON: {str(e)}")
                # Default to analysis if parsing fails
                return {"intent": "NEEDS_ANALYSIS", "analysis_query": user_message}
                
        except Exception as e:
            logger.error(f"Failed to determine intent for upload_id: {upload_id}: {str(e)}", exc_info=True)
            return {"intent": "NEEDS_ANALYSIS", "analysis_query": user_message}
            


    # -------------------
    # Generate insights - FIXED TO PREVENT HALLUCINATION
    # -------------------
    # Replace the generate_insights_response method in conversation_agent.py

    def generate_insights_response(self, upload_id: str, user_message: str, 
                            translated_results: Any, schema: Dict[str, Any]) -> str:
        """Generate natural language insights from analysis results"""
        logger.info(f"Generating insights response for upload_id: {upload_id}")
        logger.info(f"Translated results type: {type(translated_results)}, value: {translated_results}")
        
        try:
            # ============================================================
            # HANDLE TRANSLATED PERIOD RESULTS (from "which month" queries)
            # ============================================================
            if isinstance(translated_results, dict) and 'period' in translated_results:
                period = translated_results['period']
                value = translated_results['value']
                
                # Determine the metric from the query
                metric = "revenue"
                if 'budget' in user_message.lower():
                    metric = "budget"
                elif 'projected' in user_message.lower():
                    metric = "projected revenue"
                elif 'actual' in user_message.lower() or 'revenue' in user_message.lower():
                    metric = "actual revenue"
                
                return f"The highest {metric} was in **{period}** with **${value:,.2f}**"
            
            # ============================================================
            # HANDLE EMPTY RESULTS
            # ============================================================
            if translated_results is None or (isinstance(translated_results, (list, dict)) and len(translated_results) == 0):
                return "No data found matching your query."
            
            # ============================================================
            # HANDLE SIMPLE NUMERIC RESULTS
            # ============================================================
            if isinstance(translated_results, (int, float)) and not isinstance(translated_results, bool):
                # Format based on context
                if 'how many' in user_message.lower() or 'count' in user_message.lower():
                    return f"Found **{int(translated_results)}** entries."
                else:
                    return f"The result is **${translated_results:,.2f}**"
            
            # ============================================================
            # HANDLE STRING RESULTS (e.g., single customer name)
            # ============================================================
            if isinstance(translated_results, str):
                # Check if it looks like a formatted answer already
                if ':' in translated_results and '$' in translated_results:
                    return translated_results  # Already formatted
                else:
                    return f"**{translated_results}**"
            
            # ============================================================
            # HANDLE COMPLEX RESULTS (lists, dicts) - Use AI
            # ============================================================
            # Convert results to string representation
            if isinstance(translated_results, (list, dict)):
                results_str = str(translated_results)
            else:
                results_str = str(translated_results)
            
            # Build context about the query type
            query_context = ""
            if 'which' in user_message.lower() and ('highest' in user_message.lower() or 'most' in user_message.lower()):
                query_context = "This is a 'find maximum' query. Highlight the top result clearly."
            elif 'rank' in user_message.lower() or 'top' in user_message.lower():
                query_context = "This is a ranking query. Present as a ranked list."
            elif 'compare' in user_message.lower():
                query_context = "This is a comparison query. Show differences clearly."
            
            prompt = f"""
    The user asked: "{user_message}"

    The analysis returned these results:
    {results_str}

    {query_context}

    Generate a clear, concise response in natural language.

    CRITICAL FORMATTING RULES:
    1. **Use bold (**text**) for key numbers, names, and important terms**
    2. Use bullet points (- ) for lists of 3+ items
    3. **Always format currency with $ and commas** (e.g., **$12,345.67**)
    4. **If results contain technical column names (like "Actual.6", "Budget.1"), DO NOT show them to user**
    5. If results are empty, "No data found", or null, say so clearly and suggest alternatives
    6. Keep responses concise and business-friendly
    7. If showing multiple entities with values, format as: **EntityName: $value**
    8. For "which X has highest Y" queries, emphasize the winner prominently
    9. Round currency to 2 decimal places maximum
    10. If the result is a list or array, format it as a proper list with entities/values

    EXAMPLES OF GOOD RESPONSES:

    Query: "Which customer generated most revenue?"
    Result: {{"ADIB Bank": 45230.50, "NCR Pakistan": 12000.0}}
    Good Response: "**ADIB Bank** generated the most revenue with **$45,230.50**, followed by NCR Pakistan with $12,000.00"

    Query: "Which customers exceeded their budget in January?"
    Result: ['HBL', 'NCR Pakistan', 'ADIB Bank']
    Good Response: "The following customers exceeded their budget in January:
    - **HBL**
    - **NCR Pakistan**
    - **ADIB Bank**"

    Query: "Top 5 customers by revenue"
    Result: {{"ADIB": 50000, "NCR": 40000, "HBL": 30000, "MCB": 25000, "UBL": 20000}}
    Good Response: "Top 5 customers by revenue:
    - **ADIB**: $50,000
    - **NCR**: $40,000
    - **HBL**: $30,000
    - **MCB**: $25,000
    - **UBL**: $20,000"

    Query: "Total revenue"
    Result: 146007.09
    Good Response: "The total revenue is **$146,007.09**"

    Now generate the response:
    """
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=500
            )
            
            insights = response.choices[0].message.content
            logger.info(f"Generated insights: {insights}")
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate insights: {str(e)}", exc_info=True)
            # Fallback: return raw results with basic formatting
            try:
                if isinstance(translated_results, (int, float)):
                    return f"Result: **${translated_results:,.2f}**"
                elif isinstance(translated_results, list):
                    items = "\n- ".join([f"**{item}**" for item in translated_results])
                    return f"Results:\n- {items}"
                else:
                    return f"I found results but couldn't format them properly. Here's what I got: {str(translated_results)[:300]}"
            except:
                return "I found results but encountered an error formatting them."
            




    def handle_conversational_response(
        self, 
        upload_id: str, 
        user_message: str,
        schema: Dict[str, Any],
        last_analysis_results: Any = None
    ) -> str:
        """Handle purely conversational messages with proper formatting"""
        logger.info(f"Handling conversational response for upload_id: {upload_id}")
        
        try:
            history = self.get_conversation_history(upload_id)
            
            # Simplify schema but keep accuracy
            is_multi_sheet = schema.get('sheet_count', 1) > 1
            
            if is_multi_sheet:
                schema_context = {
                    'sheet_count': schema.get('sheet_count'),
                    'sheet_names': schema.get('sheet_names'),
                    'sheets': {}
                }
                
                for sheet_name, sheet_data in schema.get('sheets', {}).items():
                    basic_info = sheet_data.get('basic_info', {})
                    data_types = sheet_data.get('data_types', {})
                    
                    schema_context['sheets'][sheet_name] = {
                        'row_count': basic_info.get('total_rows'),
                        'columns': basic_info.get('column_names', []),
                        'numerical_columns': data_types.get('numerical', []),
                        'categorical_columns': data_types.get('categorical', [])
                    }
            else:
                basic_info = schema.get('basic_info', {})
                data_types = schema.get('data_types', {})
                
                schema_context = {
                    'row_count': basic_info.get('total_rows'),
                    'columns': basic_info.get('column_names', []),
                    'numerical_columns': data_types.get('numerical', []),
                    'categorical_columns': data_types.get('categorical', [])
                }
            
            prompt = f"""
You are a business intelligence assistant providing information about the dataset structure.

CONVERSATION HISTORY: {history[-2:] if history else "None"}
USER QUESTION: "{user_message}"

DATA STRUCTURE:
{json.dumps(schema_context, indent=2)}

RULES:
1. Provide clear, factual information about the dataset
2. Use natural language, not technical jargon
3. Format numbers with commas
4. Keep responses concise and business-focused
5. DO NOT make up or assume any data

Now respond to the user's question about their dataset:
"""
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=800
            )
            
            conversational_response = response.choices[0].message.content
            self.add_to_history(upload_id, user_message, conversational_response)
            return conversational_response
        
        except Exception as e:
            logger.error(f"Failed to handle conversational response: {str(e)}", exc_info=True)
            raise

    def get_conversation_history(self, upload_id: str) -> List[Dict]:
        """Get conversation history"""
        return self.conversation_history.get(upload_id, [])
    
    def add_to_history(self, upload_id: str, query: str, response: str):
        """Add exchange to conversation history"""
        if upload_id not in self.conversation_history:
            self.conversation_history[upload_id] = []
        
        self.conversation_history[upload_id].append({
            'user_message': query,
            'assistant_response': response,
            'timestamp': datetime.now().isoformat()
        })
        
        # Keep only last 20 exchanges
        if len(self.conversation_history[upload_id]) > 20:
            self.conversation_history[upload_id] = self.conversation_history[upload_id][-20:]