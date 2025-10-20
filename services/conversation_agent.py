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
   - Revenue questions (actual/projected/budget) ✅
   - Financial metrics ✅
   - Customer/product analysis ✅
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
    
    def generate_insights_from_sql(self, upload_id: str, user_question: str, sql_results: list, metadata: dict = None) -> str:
        """Generate response using ONLY provided metadata - NO hallucinations"""
        
        logger.info("Generating insights from SQL results")
        logger.info(f"Metadata provided: {metadata}")
        
        # Extract filter details
        filters = metadata.get('filters_applied', {}) if metadata else {}
        metric = metadata.get('metric_used', 'revenue') if metadata else 'revenue'
        aggregation = metadata.get('aggregation', 'sum') if metadata else 'sum'
        group_by = metadata.get('group_by', []) if metadata else []
        
        # Build filter description
        filter_desc = []
        if filters.get('year'):
            filter_desc.append(f"Year: {filters['year']}")
        if filters.get('month'):
            filter_desc.append(f"Month: {filters['month']}")
        if filters.get('months'):
            filter_desc.append(f"Months: {', '.join(filters['months'])}")
        if filters.get('customer'):
            filter_desc.append(f"Customer: {filters['customer']}")
        if filters.get('region'):
            filter_desc.append(f"Region: {filters['region']}")
        if filters.get('quarter'):
            filter_desc.append(f"Quarter: {filters['quarter']}")
        
        filter_summary = " | ".join(filter_desc) if filter_desc else "All data"
        
        system_prompt = f"""You are a financial analyst. Present SQL results clearly.

## CRITICAL RULES:
1. Use ONLY the provided metadata - NEVER invent years, months, or filters
2. State ALL filters that were applied
3. If data is grouped by year, show breakdown by year
4. Format numbers with currency and thousands separators

## PROVIDED METADATA:
Metric: {metric}
Filters Applied: {filter_summary}
Grouped By: {', '.join(group_by) if group_by else 'None'}
Aggregation: {aggregation}

## RESPONSE FORMAT:
**[Main Answer with numbers from results]**

**Details:**
• Metric: {metric.capitalize()} Revenue
• Filters: {filter_summary}
{f"• Breakdown: By {', '.join(group_by)}" if group_by else ""}
• Calculation: {aggregation.upper()} of {metric} values

## EXAMPLES:

If grouped by year:
"**Revenue Breakdown:**
• 2023: $5,000
• 2024: $6,500  
• 2025: $7,200

**Details:**
• Metric: Actual Revenue
• Filters: Month: MAR
• Breakdown: By year
• Calculation: SUM of monthly actual values"

If single value:
"**Total: $18,700**

**Details:**
• Metric: Projected Revenue
• Filters: Year: 2025 | Quarter: Q1
• Calculation: SUM of monthly projected values"

## NEVER:
- Invent years (use only from results)
- Assume filters (state only provided filters)
- Add extra explanations not based on data
"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"""User Question: {user_question}

SQL Results: {json.dumps(sql_results, indent=2)}

Format into transparent response."""}
                ],
                temperature=0.3
            )
            
            formatted_response = response.choices[0].message.content
            
            # Store in history
            if upload_id not in self.conversation_history:
                self.conversation_history[upload_id] = []
            
            self.conversation_history[upload_id].append({
                'question': user_question,
                'answer': formatted_response,
                'timestamp': datetime.now().isoformat()
            })
            
            if len(self.conversation_history[upload_id]) > 10:
                self.conversation_history[upload_id] = self.conversation_history[upload_id][-10:]
            
            return formatted_response
            
        except Exception as e:
            logger.error(f"Error generating insights: {str(e)}")
            if sql_results:
                return f"Results: {json.dumps(sql_results, indent=2)}\n\n(Based on {metric} revenue with filters: {filter_summary})"
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
    
    def get_conversation_history(self, upload_id: str) -> list:
        """Get conversation history"""
        return self.conversation_history.get(upload_id, [])
    
    def clear_history(self, upload_id: str):
        """Clear conversation history"""
        if upload_id in self.conversation_history:
            del self.conversation_history[upload_id]
            logger.info(f"History cleared for: {upload_id}")