import os
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime
import logging
import json

load_dotenv()
logger = logging.getLogger(__name__)

class ConversationAgent:
    """Handles conversational responses with NO hallucinations"""
    
    def __init__(self):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found")
        self.client = OpenAI(api_key=api_key)
        self.conversation_history = {}
    
    def determine_intent_sql(self, upload_id: str, user_message: str) -> dict:
        """Determine intent - recognizing budget/projected/actual as valid"""
        
        logger.info(f"Determining intent for: {user_message}")
        
        system_prompt = """You are an intent classifier for a revenue analysis chatbot.

Classify into:

1. **NEEDS_ANALYSIS**: Questions requiring database queries
   - Revenue questions (actual/projected/budget)
   - Financial metrics
   - Customer/product analysis
   - Examples: "total budget?", "projected revenue?", "actual sales?", "top customers"
   
2. **CONVERSATIONAL**: General chat
   - Greetings, thanks, capabilities
   
3. **OUT_OF_SCOPE**: Unrelated questions
   - Weather, news, jokes

## CRITICAL: 
- Budget/Projected/Actual are ALL valid metrics = NEEDS_ANALYSIS
- "analysis_query" = natural language (NOT SQL!)

Return JSON:
{
  "intent": "NEEDS_ANALYSIS|CONVERSATIONAL|OUT_OF_SCOPE",
  "analysis_query": "clear natural language question",
  "reason": "brief explanation"
}
"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            logger.info(f"Intent: {result['intent']}")
            
            # Validate no SQL in analysis_query
            if result.get('intent') == 'NEEDS_ANALYSIS' and result.get('analysis_query'):
                query = result['analysis_query'].upper()
                if any(kw in query for kw in ['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE']):
                    logger.warning("Intent returned SQL, using original message")
                    result['analysis_query'] = user_message
            
            return result
            
        except Exception as e:
            logger.error(f"Error determining intent: {str(e)}")
            return {
                'intent': 'OUT_OF_SCOPE',
                'reason': f'Error: {str(e)}'
            }
    
    def generate_insights_from_sql(self, upload_id: str, user_question: str, sql_results: list, metadata: dict = None, validation: dict = None) -> str:
        
        
        warning_message = None
        
        """Generate response using ONLY provided metadata - NO hallucinations"""
        
        logger.info("Generating insights from SQL results")
        logger.info(f"Metadata provided: {metadata}")
        logger.info(f"Validation: {validation}")
        
        # Handle validation failures or warnings
        if validation:
            if not validation.get('is_valid', True):
                # Validation failed - return helpful message
                return validation.get('message', 'No data found matching your query.')
            
            if validation.get('has_warning', False):
                # Has warning - we'll append it to the response
                warning_message = validation.get('warning', '')
        
        
        # Extract filter details
        filters = metadata.get('filters_applied', {}) if metadata else {}
        metric = metadata.get('metric_used', 'revenue') if metadata else 'revenue'
        aggregation = metadata.get('aggregation', 'sum') if metadata else 'sum'
        group_by = metadata.get('group_by', []) if metadata else []
        
        # Build filter description

        filter_desc = []

        # Temporal filters
        if filters.get('year'):
            filter_desc.append(f"Year: {filters['year']}")
        if filters.get('month'):
            filter_desc.append(f"Month: {filters['month']}")
        if filters.get('months'):
            filter_desc.append(f"Months: {', '.join(filters['months'])}")
        if filters.get('quarter'):
            filter_desc.append(f"Quarter: {filters['quarter']}")

        # Organizational filters
        if filters.get('unit'):
            if isinstance(filters['unit'], list):
                filter_desc.append(f"Units: {', '.join(filters['unit'])}")
            else:
                filter_desc.append(f"Unit: {filters['unit']}")

        if filters.get('category'):
            filter_desc.append(f"Category: {filters['category']}")

        # Geographic filters
        if filters.get('region'):
            filter_desc.append(f"Region: {filters['region']}")

        if filters.get('country'):
            filter_desc.append(f"Country: {filters['country']}")

        # Business entity filters
        if filters.get('customer'):
            filter_desc.append(f"Customer: {filters['customer']}")

        if filters.get('product'):
            filter_desc.append(f"Product: {filters['product']}")

        if filters.get('project_code'):
            filter_desc.append(f"Project: {filters['project_code']}")

        filter_summary = " | ".join(filter_desc) if filter_desc else "All data"
        # ✅ NEW: COMPARISON MODE RESPONSE HANDLING (no LLM needed)
        # If grouped by only one entity AND we have multiple rows → this is a comparison query
        if group_by and len(group_by) == 1 and len(sql_results) > 1:
            group_column = group_by[0]
            metric_col = metric if metric in sql_results[0] else "revenue"

            # Convert results into (name, value)
            comparison_pairs = []
            for row in sql_results:
                name = str(row.get(group_column, "Unknown"))
                value = float(row.get(metric_col, 0) or 0)
                comparison_pairs.append((name, value))

            # Sort highest → lowest
            comparison_pairs.sort(key=lambda x: x[1], reverse=True)

            # Build response
            response = "**Revenue Comparison:**\n\n"
            for name, value in comparison_pairs:
                response += f"- **{name}**: ${value:,.2f}\n"

            if len(comparison_pairs) == 2:
                (n1, v1), (n2, v2) = comparison_pairs
                diff = v1 - v2
                pct = (abs(diff) / v2 * 100) if v2 != 0 else 0
                multiple = (v1 / v2) if v2 != 0 else float('inf')
                direction = "higher" if diff > 0 else "lower"

                response += (
                    f"\n**Difference:**\n"
                    f"→ {n1} is **${abs(diff):,.2f} ({pct:.1f}%)** {direction} than {n2}\n"
                    f"→ That is **{multiple:.2f}×** the value of {n2}\n"
                )

            # Store history
            if upload_id not in self.conversation_history:
                self.conversation_history[upload_id] = []
            self.conversation_history[upload_id].append({
                'question': user_question,
                'answer': response,
                'metadata': metadata,   # ✅ STORE METADATA
                'sql_results': sql_results,
                'timestamp': datetime.now().isoformat()
            })

            return response  # ✅ Return early — NO LLM CALL needed

        # ✅ NEW: FALLBACK COMPARISON USING PREVIOUS RESULT
        if upload_id in self.conversation_history and "compare" in user_question.lower():
            
            # Look for the most recent previous result with numeric value
            prev_result = None
            for past in reversed(self.conversation_history[upload_id]):
                if past.get("sql_results") and len(past["sql_results"]) == 1:
                    prev_result = past["sql_results"][0]
                    break

            if prev_result and len(sql_results) == 1:

                # Identify numeric column in previous and current rows
                def extract_numeric(row):
                    for k, v in row.items():
                        try:
                            return float(v)
                        except:
                            continue
                    return None

                previous_value = extract_numeric(prev_result)
                current_value = extract_numeric(sql_results[0])

                if previous_value is None or current_value is None:
                    return "Comparison failed — unable to find numeric values."

                # Compute differences
                diff = current_value - previous_value
                pct = (abs(diff) / previous_value * 100) if previous_value != 0 else 0
                multiple = (current_value / previous_value) if previous_value != 0 else float('inf')
                direction = "higher" if diff > 0 else "lower"

                response = f"""**Comparison Result**

- Previous: ${previous_value:,.2f}
- Current: ${current_value:,.2f}

**Difference:**  
→ Current is **${abs(diff):,.2f} ({pct:.1f}%)** {direction} than previous  
→ That is **{multiple:.2f}×** the previous value
"""

                # ✅ Store the comparison result in history
                self.conversation_history[upload_id].append({
                    'question': user_question,
                    'answer': response,
                    'metadata': metadata,
                    'sql_results': sql_results,
                    'timestamp': datetime.now().isoformat()
                })

                return response


        system_prompt = f"""You are a financial analyst. Present SQL results clearly.

## CRITICAL RULES:
1. Use ONLY the provided metadata - NEVER invent years, months, or filters
2. State ALL filters that were applied
3. If data is grouped by year, show breakdown by year
4. Format numbers with currency and thousands separators
5. If results are empty or zero, state this clearly
6. **ALWAYS include entity names (unit/product/customer/region) in responses when present in results**

## PROVIDED METADATA:
Metric: {metric}
Filters Applied: {filter_summary}
Grouped By: {', '.join(group_by) if group_by else 'None'}
Aggregation: {aggregation}

## RESPONSE FORMAT:
**[Main Answer with numbers from results]**

**Details:**
- Metric: {metric.capitalize()} Revenue
- Filters: {filter_summary}
{f"- Breakdown: By {', '.join(group_by)}" if group_by else ""}
- Calculation: {aggregation.upper()} of {metric} values

## EXAMPLES:

If grouped by year:
"**Revenue Breakdown:**
- 2023: $5,000
- 2024: $6,500  
- 2025: $7,200

**Details:**
- Metric: Actual Revenue
- Filters: Month: MAR
- Breakdown: By year
- Calculation: SUM of monthly actual values"

If single value:
"**Total: $18,700**

**Details:**
- Metric: Projected Revenue
- Filters: Year: 2025 | Quarter: Q1
- Calculation: SUM of monthly projected values"

If comparison (grouped by unit/region/customer):
"**Revenue Comparison:**
- AMS: $50,000
- CRM: $45,000
- BSD: $35,000

**Details:**
- Metric: Actual Revenue
- Filters: All data
- Breakdown: By unit
- Calculation: SUM of actual values"

**CRITICAL: For "which X had highest/most" queries:**
"**Highest Revenue Unit: AMS - $50,000**

**Details:**
- Metric: Actual Revenue
- Filters: All data
- Breakdown: By unit
- Calculation: SUM of actual values"

"**Top Customer: ADIB Bank - $125,000**

**Details:**
- Metric: Actual Revenue
- Filters: All data
- Breakdown: By customer
- Calculation: SUM of actual values"

"**Best-Selling Product: Novus ATM Controller - $75,000**

**Details:**
- Metric: Actual Revenue
- Filters: All data
- Breakdown: By product
- Calculation: SUM of actual values"

If percentage result:
"**Target Achievement: 87.5%**

**Details:**
- Calculation: (Actual / Budget) × 100
- Filters: Year: 2025
- Performance: Currently at 87.5% of annual target"

## NEVER:
- Invent years (use only from results)
- Assume filters (state only provided filters)
- Add extra explanations not based on data
- **Omit entity names when they exist in results (unit, product, customer, region, category)**
"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"""User Question: {user_question}

SQL Results: {json.dumps(sql_results, indent=2)}

Format into transparent response. **CRITICAL: If results contain entity names (unit, product, customer, region), ALWAYS include them in the response.**"""}
                ],
                temperature=0.3
            )
            
            formatted_response = response.choices[0].message.content
            
            # Append warning if present
            if warning_message:
                formatted_response += f"\n\n **Note:** {warning_message}"
            
            # Store in history
            if upload_id not in self.conversation_history:
                self.conversation_history[upload_id] = []
            
            self.conversation_history[upload_id].append({
                'question': user_question,
                'answer': formatted_response,
                'metadata': metadata,
                'sql_results': sql_results,
                'timestamp': datetime.now().isoformat()
            })
            
            if len(self.conversation_history[upload_id]) > 10:
                self.conversation_history[upload_id] = self.conversation_history[upload_id][-10:]
            
            return formatted_response
            
        except Exception as e:
            logger.error(f"Error generating insights: {str(e)}")
            if sql_results:
                base_response = f"Results: {json.dumps(sql_results, indent=2)}\n\n(Based on {metric} revenue with filters: {filter_summary})"
                if warning_message:
                    base_response += f"\n\n {warning_message}"
                return base_response
            else:
                return "No results found."
    
    def handle_conversational_sql(self, upload_id: str, user_message: str) -> str:
        """Handle conversational messages"""
        
        logger.info("Handling conversational message")
        
        system_prompt = """You are a friendly AI assistant for revenue analysis.

Respond naturally to:
- Greetings
- Thanks
- Capability questions

Keep responses brief. Remind users you analyze revenue data (actual/projected/budget).

Example responses:
- "Hello! I can help you analyze your revenue data. Try asking about actual revenue, budget performance, or customer comparisons."
- "You're welcome! Feel free to ask any questions about your revenue data."
- "I can answer questions about revenue metrics (actual, projected, budget), compare performance across units/regions/customers, identify trends, and much more. What would you like to know?"
"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.7
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            return "I'm here to help analyze your revenue data. What would you like to know?"

    def add_message(self, upload_id, question, answer, metadata=None, sql_results=None):
        if upload_id not in self.conversation_history:
            self.conversation_history[upload_id] = []

        self.conversation_history[upload_id].append({
            "question": question,
            "answer": answer,
            "metadata": metadata or {},
            "sql_results": sql_results,
            "timestamp": datetime.now().isoformat()
        })


    def get_conversation_history(self, upload_id: str) -> list:
        """Get conversation history"""
        return self.conversation_history.get(upload_id, [])
    
    def clear_history(self, upload_id: str):
        """Clear conversation history"""
        if upload_id in self.conversation_history:
            del self.conversation_history[upload_id]
            logger.info(f"History cleared for: {upload_id}")