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
            
            # Get available columns for context
            if schema.get('sheet_count', 1) > 1:
                available_columns = schema.get('all_columns', [])
                sheet_info = f"Multi-sheet data with sheets: {schema.get('sheet_names', [])}"
            else:
                available_columns = schema.get('basic_info', {}).get('column_names', [])
                sheet_info = f"Single sheet with {len(available_columns)} columns"
            
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
            
            prompt = f"""
You are analyzing a user's message in a data analysis context.

DATASET INFO: {sheet_info}
AVAILABLE COLUMNS: {available_columns}

RECENT CONVERSATION:
{context_str if context_str else "No previous conversation"}

NEW USER MESSAGE: "{user_message}"

CRITICAL SCOPE RULES:
1. The user ONLY has access to data with columns: {available_columns}
2. Questions must be DIRECTLY answerable using ONLY these columns
3. General knowledge questions (e.g., "who is donald trump", "what is python") are OUT OF SCOPE
4. Questions about columns/data NOT in the available columns are OUT OF SCOPE
5. If asking about entities not in the data, mark as OUT_OF_SCOPE

YOUR TASKS:
1. First check: Is this query IN SCOPE or OUT OF SCOPE?
   - IN SCOPE: Can be answered with the available columns
   - OUT OF SCOPE: Requires data not in the columns OR general knowledge

2. If IN SCOPE, determine intent:
   - NEEDS_ANALYSIS: Requires data calculations/aggregations
   - CONVERSATIONAL: Asks about dataset structure, explanations, clarifications

3. CRITICAL: Replace contextual references with actual values from recent conversation:
   - "this rep", "that region", "these items" → Replace with actual names
   - "from this", "in that", "about this" → Replace with specific values

EXAMPLES:

Example 1 - OUT OF SCOPE:
User: "who is donald trump"
→ {{"intent": "OUT_OF_SCOPE", "analysis_query": "", "reason": "General knowledge question, not about the dataset"}}

Example 2 - OUT OF SCOPE:
User: "show me customer emails"
Available columns: ['OrderDate', 'Region', 'Rep']
→ {{"intent": "OUT_OF_SCOPE", "analysis_query": "", "reason": "Dataset does not contain email addresses"}}

Example 3 - IN SCOPE with context:
Recent: "Q: pencils in East region A: 130 units sold"
User: "which dates were these"
→ {{"intent": "NEEDS_ANALYSIS", "analysis_query": "which dates were pencils sold in East region"}}

Example 4 - IN SCOPE with context:
Recent: "Q: best rep in Central? A: Kivell"
User: "how many units did he sell"
→ {{"intent": "NEEDS_ANALYSIS", "analysis_query": "how many units did Kivell sell"}}

Example 5 - IN SCOPE simple:
User: "total sales in East region"
→ {{"intent": "NEEDS_ANALYSIS", "analysis_query": "total sales in East region"}}

Example 6 - CONVERSATIONAL:
User: "what columns do I have"
→ {{"intent": "CONVERSATIONAL", "analysis_query": "what columns do I have"}}

Return ONLY valid JSON:
{{"intent": "NEEDS_ANALYSIS" | "CONVERSATIONAL" | "OUT_OF_SCOPE", "analysis_query": "resolved query", "reason": "if OUT_OF_SCOPE"}}
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
    def generate_insights_response(
        self, 
        upload_id: str,
        user_query: str, 
        analysis_results: Any,
        schema: Dict[str, Any],
        precision_mode: bool = False
    ) -> str:
        """Generate well-formatted CEO-focused insights - STRICTLY based on raw results"""
        logger.info(f"Generating insights response for upload_id: {upload_id}")
        logger.info(f"Raw results type: {type(analysis_results)}, value: {analysis_results}")
        
        try:
            history = self.get_conversation_history(upload_id)

            if isinstance(analysis_results, dict):
                formatted_results = json.dumps(analysis_results, indent=2)
            else:
                formatted_results = str(analysis_results)

            prompt = f"""
You are presenting data analysis results to a business user.

USER ASKED: "{user_query}"

RAW ANALYSIS RESULT: {formatted_results}

CRITICAL RULES - PREVENT HALLUCINATION:
1. Use ONLY the data in "RAW ANALYSIS RESULT" above
2. DO NOT invent, assume, or add ANY information not in the raw result
3. DO NOT add percentages, metrics, or details unless they are in the raw result
4. DO NOT make up names, numbers, or explanations
5. If the raw result is just a name/number, present it simply without elaboration
6. If the raw result is a list/dict, format it clearly but add NO new data

FORMATTING GUIDELINES:
1. Start with a direct answer
2. Format numbers with commas (e.g., 1,234)
3. Use bullet points ONLY when raw result has multiple items
4. Keep response under 100 words unless showing a list
5. Be concise and factual

EXAMPLES:

Query: "best rep in east region"
Raw Result: "Jones"
GOOD Response: "The best performing rep in the East region is **Jones**."
BAD Response: "The best rep is Alex Johnson with $1.2M sales and 15% growth..." ← HALLUCINATION

Query: "total units in Central"
Raw Result: 1199
GOOD Response: "Total units sold in the Central region: **1,199 units**."
BAD Response: "1,199 units with strong growth and high customer satisfaction..." ← HALLUCINATION

Query: "dates of pencil sales"
Raw Result: ["2024-01-04", "2024-01-09", "2024-02-26"]
GOOD Response: "Pencils were sold on these dates:\n- January 4, 2024\n- January 9, 2024\n- February 26, 2024"
BAD Response: Lists dates not in the raw result ← HALLUCINATION

Now respond using ONLY the raw result data, with NO additions:
"""
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,  # Lower temperature to reduce creativity
                max_tokens=500
            )
            
            insights = response.choices[0].message.content
            logger.info(f"Generated insights: {insights[:200]}")
            self.add_to_history(upload_id, user_query, insights)
            return insights
        
        except Exception as e:
            logger.error(f"Failed to generate insights: {str(e)}", exc_info=True)
            # Fallback to raw result if insights generation fails
            return f"Analysis result: {analysis_results}"

    # ... rest of the methods remain the same ...
    
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