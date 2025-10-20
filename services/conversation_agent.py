# app/services/conversation_agent.py
from openai import OpenAI
import json
from config.settings import get_settings
from typing import Dict, Any, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

settings = get_settings()

class ConversationAgent:
    """AI agent for natural conversation with SQL-based backend"""
    
    def __init__(self):
        try:
            self.client = OpenAI(api_key=settings.openai_api_key)
            self.conversation_history = {}
            logger.info("Conversation Agent initialized (SQL mode)")
        except Exception as e:
            logger.error(f"Failed to initialize Conversation Agent: {str(e)}")
            raise
    
    def determine_intent_sql(self, upload_id: str, user_message: str) -> Dict[str, Any]:
        """
        Determine if user needs analysis or conversation (SQL-based)
        """
        logger.info(f"Determining intent for: {user_message}")
        
        try:
            history = self.get_conversation_history(upload_id)
            
            # Build context
            context_str = ""
            if history:
                recent = history[-2:]
                context_parts = []
                for exchange in recent:
                    user_q = exchange.get('user_message', '')
                    assistant_a = exchange.get('assistant_response', '')[:200]
                    context_parts.append(f"Q: {user_q}\nA: {assistant_a}")
                context_str = "\n\n".join(context_parts)
            
            prompt = f"""
You are analyzing a user's message in a revenue tracking context.

DATABASE SCHEMA:
- unit: Business units (AMS, ME Support, Sales Force, BSD)
- product: Product names
- region: Geographic regions (ME, AFR, PK)
- country: Country names
- customer: Customer names
- category: Project categories (Funnel, New Sales, WIH)
- month: Month codes (JAN, FEB, MAR, etc.)
- budget, projected, actual: Revenue metrics
- Summary fields: ytd_actual, total, wih_2024, etc.

RECENT CONVERSATION:
{context_str if context_str else "No previous conversation"}

USER MESSAGE: "{user_message}"

TASK:
1. Is this IN SCOPE or OUT OF SCOPE?
   - IN SCOPE: Questions about revenue, budget, customers, regions, etc.
   - OUT OF SCOPE: General knowledge, unrelated topics
   
2. If IN SCOPE, determine:
   - NEEDS_ANALYSIS: Requires data query (aggregations, comparisons, rankings)
   - CONVERSATIONAL: Asks about capabilities, clarifications, explanations

3. Resolve contextual references:
   - Replace "this", "that", "these" with actual entities from context

Return ONLY valid JSON:
{{
    "intent": "NEEDS_ANALYSIS" | "CONVERSATIONAL" | "OUT_OF_SCOPE",
    "analysis_query": "resolved query" or null,
    "reason": "explanation if OUT_OF_SCOPE"
}}

EXAMPLES:

Input: "What's the total actual revenue for AMS?"
Output: {{"intent": "NEEDS_ANALYSIS", "analysis_query": "total actual revenue for AMS unit"}}

Input: "Which customers are in the ME region?"
Output: {{"intent": "NEEDS_ANALYSIS", "analysis_query": "list customers in ME region"}}

Input: "What columns are available?"
Output: {{"intent": "CONVERSATIONAL", "analysis_query": null}}

Input: "Who is Donald Trump?"
Output: {{"intent": "OUT_OF_SCOPE", "reason": "Question is not related to revenue data"}}
"""
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=300
            )
            
            raw_response = response.choices[0].message.content
            
            # Parse JSON
            if "```json" in raw_response:
                raw_response = raw_response.split("```json")[1].split("```")[0]
            elif "```" in raw_response:
                raw_response = raw_response.split("```")[1].split("```")[0]
            
            intent_result = json.loads(raw_response.strip())
            logger.info(f"Intent determined: {intent_result['intent']}")
            
            return intent_result
            
        except Exception as e:
            logger.error(f"Intent determination failed: {str(e)}", exc_info=True)
            return {"intent": "NEEDS_ANALYSIS", "analysis_query": user_message}
    
    def generate_insights_from_sql(self, upload_id: str, user_message: str, 
                                   sql_result: Any) -> str:
        """
        Generate natural language insights from SQL results
        """
        logger.info(f"Generating insights from SQL result")
        logger.info(f"Result type: {type(sql_result)}, value: {sql_result}")
        
        try:
            # Handle empty results
            if sql_result is None or (isinstance(sql_result, (list, dict)) and len(sql_result) == 0):
                return "No data found matching your query."
            
            # Handle simple numeric results
            if isinstance(sql_result, (int, float)) and not isinstance(sql_result, bool):
                if 'count' in user_message.lower() or 'how many' in user_message.lower():
                    return f"Found **{int(sql_result)}** entries."
                else:
                    return f"The result is **${sql_result:,.2f}**"
            
            # Handle string results
            if isinstance(sql_result, str):
                return f"**{sql_result}**"
            
            # Handle complex results - use AI
            results_str = str(sql_result)
            
            prompt = f"""
The user asked: "{user_message}"

The SQL query returned: {results_str}

Generate a clear, concise response in natural language.

FORMATTING RULES:
1. Use bold (**text**) for numbers, names, and key terms
2. Format currency with $ and commas (e.g., **$12,345.67**)
3. Use bullet points for lists of 3+ items
4. Keep responses business-friendly and concise
5. If showing multiple items, format as: **Name: $value**

EXAMPLES:

Query: "Which customer has highest revenue?"
Result: {{"customer": "ADIB Bank", "total_revenue": 45230.50}}
Response: "**ADIB Bank** has the highest revenue with **$45,230.50**"

Query: "Total revenue by region"
Result: [{{"region": "ME", "total": 50000}}, {{"region": "AFR", "total": 30000}}]
Response: "Revenue by region:
- **ME**: $50,000
- **AFR**: $30,000"

Now generate the response:
"""
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=500
            )
            
            insights = response.choices[0].message.content
            
            # Add to conversation history
            self.add_to_history(upload_id, user_message, insights)
            
            return insights
            
        except Exception as e:
            logger.error(f"Insights generation failed: {str(e)}", exc_info=True)
            return f"I found results but encountered an error formatting them: {str(sql_result)[:200]}"
    
    def handle_conversational_sql(self, upload_id: str, user_message: str) -> str:
        """Handle conversational messages"""
        logger.info(f"Handling conversational message")
        
        try:
            history = self.get_conversation_history(upload_id)
            
            prompt = f"""
You are a helpful AI assistant for a revenue tracking system.

The system has the following data:
- Business units, products, regions, countries, customers
- Monthly revenue data (budget, projected, actual)
- 12 months of data for 2025

CONVERSATION HISTORY: {history[-2:] if history else "None"}
USER QUESTION: "{user_message}"

Provide a helpful, friendly response about the system's capabilities.

EXAMPLES:
- "What can I ask?" → Explain types of queries (totals, comparisons, rankings)
- "What data do you have?" → Describe the revenue tracking data structure
- "How does this work?" → Explain the query process
"""
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=300
            )
            
            conversational_response = response.choices[0].message.content
            self.add_to_history(upload_id, user_message, conversational_response)
            
            return conversational_response
            
        except Exception as e:
            logger.error(f"Conversational response failed: {str(e)}", exc_info=True)
            return "I'm here to help you analyze your revenue data. What would you like to know?"
    
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