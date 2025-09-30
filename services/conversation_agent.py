# app/services/conversation_agent.py
from openai import OpenAI
import json
from config.settings import get_settings
from typing import Dict, Any, List
from datetime import datetime
import os
import logging

# Configure logging
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
    # Intent determination
    # -------------------
    def determine_intent(self, upload_id: str, user_message: str) -> Dict[str, Any]:
        """Determine if user needs new analysis or just conversation"""
        logger.info(f"Determining intent for upload_id: {upload_id}, message: {user_message}")
        
        try:
            history = self.get_conversation_history(upload_id)
            
            prompt = f"""
            You are analyzing a user's message in a data conversation context.
            
            CONVERSATION HISTORY: {history[-3:] if history else "No previous conversation"}
            USER MESSAGE: "{user_message}"
            
            Determine the user's intent:
            1. NEEDS_ANALYSIS: User is asking for new data analysis/calculations
            2. CONVERSATIONAL: User wants explanation, discussion, or clarification about previous results
            
            Return JSON format:
            {{"intent": "NEEDS_ANALYSIS" or "CONVERSATIONAL", "analysis_query": "refined query for analysis if needed"}}
            """
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            raw_response = response.choices[0].message.content
            
            try:
                intent_result = json.loads(raw_response)
                return intent_result
            except json.JSONDecodeError:
                # Default to analysis if parsing fails
                return {"intent": "NEEDS_ANALYSIS", "analysis_query": user_message}
                
        except Exception as e:
            logger.error(f"Failed to determine intent for upload_id: {upload_id}: {str(e)}", exc_info=True)
            return {"intent": "NEEDS_ANALYSIS", "analysis_query": user_message}

    # -------------------
    # Generate insights
    # -------------------
    def generate_insights_response(
    self, 
    upload_id: str,
    user_query: str, 
    analysis_results: Any,
    schema: Dict[str, Any],
    precision_mode: bool = False
) -> str:
        """Generate well-formatted CEO-focused insights"""
        logger.info(f"Generating insights response for upload_id: {upload_id}")
        
        try:
            history = self.get_conversation_history(upload_id)

            if isinstance(analysis_results, dict):
                formatted_results = json.dumps(analysis_results, indent=2)
            else:
                formatted_results = str(analysis_results)

            prompt = f"""
    You are a business analyst presenting insights to a CEO in a chat interface.

    CEO ASKED: "{user_query}"

    RAW DATA: {formatted_results}

    PREVIOUS CONTEXT: {history[-2:] if history else "None"}

    FORMATTING REQUIREMENTS:
    1. Start with a direct answer to the question
    2. Use natural, conversational business language
    3. Format numbers with commas and currency symbols where appropriate
    4. Use bullet points ONLY when showing multiple items
    5. Keep total response under 150 words unless showing a list
    6. Be concise and actionable

    EXAMPLES:

    Query: "give me data about Kivell"
    Good Response: 
    "Kivell has 2 recorded orders:

    **Order 1 (Jan 23, 2024)**
    - Item: Binder
    - Quantity: 50 units
    - Total: $999.50

    **Order 2 (Nov 25, 2024)**
    - Item: Pen Set
    - Quantity: 27 units  
    - Total: $719.73"

    Bad Response:
    "OrderDate: 1/23/2024, Item: Binder, Units: 50, UnitCost: 19.99, Total: 999.50..."

    Query: "total sales in government"
    Good Response: "Total sales in the Government segment: **$52,504,260.67**"

    Bad Response: "The number of sales in the government sector is 470673.50"

    Now respond to the CEO's query using these formatting guidelines.
    """
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=800
            )
            
            insights = response.choices[0].message.content
            self.add_to_history(upload_id, user_query, insights)
            return insights
        
        except Exception as e:
            logger.error(f"Failed to generate insights: {str(e)}", exc_info=True)
            raise



    # -------------------
    # Conversational handling
    # -------------------
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
    You are a business intelligence assistant. Provide clear, well-formatted responses.

    CONVERSATION HISTORY: {history[-2:] if history else "None"}
    USER QUESTION: "{user_message}"

    DATA STRUCTURE:
    {json.dumps(schema_context, indent=2)}

    FORMATTING RULES:
    1. Use natural language, not raw statistics
    2. For "describe sheets" queries, provide:
    - Sheet name with brief description
    - Number of rows
    - Main column categories in plain English
    3. Use bullet points or numbered lists ONLY when listing items
    4. Keep responses concise and business-focused
    5. Avoid technical jargon like "mean", "std", "unique values" unless specifically asked
    6. Format numbers with commas (e.g., 1,234 not 1234)

    EXAMPLE GOOD RESPONSE for "describe both sheets":
    "Your file contains 2 sheets:

    **Sheet1 - Order Data**
    Contains 700 rows of order information with columns like Region, Rep, Item, Units, and pricing details.

    **Sheet2 - Financial Data**  
    Contains 500 rows of financial performance data including Sales, Profit, Country, Segment, and Product information."

    EXAMPLE BAD RESPONSE:
    "Sheet1: 43 entries, 43 unique, most frequent: 1717200000000 (1 time), mean: 49.32558139..."

    Now respond to the user's question following these formatting rules.
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



    # -------------------
    # Conversation history
    # -------------------
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
