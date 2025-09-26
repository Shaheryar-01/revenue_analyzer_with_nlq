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
        """Generate CEO-focused insights from analysis results"""
        logger.info(f"Generating insights response for upload_id: {upload_id}")
        
        try:
            history = self.get_conversation_history(upload_id)
            file_info = schema.get('basic_info', {})

            # --- Use JSON for exact numeric results ---
            if isinstance(analysis_results, dict):
                formatted_results = json.dumps(analysis_results, indent=2)
            else:
                formatted_results = str(analysis_results)

            # Detect reasoning questions
            reasoning_triggers = ["how did you find", "how did you reach", "explain how", "what led to this", "why is that", "reason for"]
            is_reasoning_question = any(trigger in user_query.lower() for trigger in reasoning_triggers)

            # --- Prompt selection ---
            if is_reasoning_question:
                prompt = f"""
                You are a business analyst responding to the CEO.
                
                CEO asked: "{user_query}"
                
                DATA: {formatted_results}
                
                TASK:
                - Explain clearly why one entity outperformed another
                - Reference exact numeric values from DATA
                - Highlight trends, comparisons, or anomalies
                - Present insights in a CEO-friendly business perspective
                - Do NOT invent any numbers or data not in DATA
                """
            elif precision_mode:
                prompt = f"""
                You are a financial analyst responding to the CEO.
                
                CEO asked: "{user_query}"
                
                DATA: {formatted_results}
                
                TASK:
                - Present numbers exactly as in DATA
                - Show full precision, commas, and two decimals
                - Do not round or approximate
                - Provide concise, factual answer without extra explanation
                """
            else:
                prompt = f"""
                You are a senior business analyst presenting insights to a CEO.
                
                CEO asked: "{user_query}"
                
                DATA: {formatted_results}
                
                CONTEXT:
                - Dataset: {file_info.get('total_rows', 'unknown')} rows, {file_info.get('total_columns', 'unknown')} columns
                - Previous conversation: {history[-2:] if history else "None"}
                
                TASK:
                - Start with direct answer to the question
                - Use exact numbers from DATA
                - Highlight business implications
                - Suggest follow-up questions if relevant
                - Maintain professional and conversational tone
                - Do NOT invent new data
                """
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            
            insights = response.choices[0].message.content
            self.add_to_history(upload_id, user_query, insights)
            return insights
        
        except Exception as e:
            logger.error(f"Failed to generate insights for upload_id: {upload_id}: {str(e)}", exc_info=True)
            raise

    # -------------------
    # Conversational handling
    # -------------------
    def handle_conversational_response(
        self, 
        upload_id: str, 
        user_message: str,
        schema: Dict[str, Any],
        last_analysis_results: Any = None  # <-- FIX: pass previous numeric data
    ) -> str:
        """Handle purely conversational messages with reference to previous analysis"""
        logger.info(f"Handling conversational response for upload_id: {upload_id}")
        
        try:
            history = self.get_conversation_history(upload_id)
            
            # Include last analysis numeric data if available
            data_info = json.dumps(last_analysis_results, indent=2) if last_analysis_results else "No previous data"
            
            prompt = f"""
            Continue this business conversation with a CEO about their data.
            
            CONVERSATION HISTORY: {history}
            CEO SAYS: "{user_message}"
            
            DATA: {data_info}
            
            CONTEXT: You've been analyzing the CEO's data and providing insights.
            
            TASK:
            - Be helpful and insightful
            - Reference previous analysis if relevant
            - Ask clarifying questions if appropriate
            - Provide business advice when suitable
            - Maintain professional but conversational tone
            - Do NOT make up new numbers
            """
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.4
            )
            
            conversational_response = response.choices[0].message.content
            self.add_to_history(upload_id, user_message, conversational_response)
            return conversational_response
        
        except Exception as e:
            logger.error(f"Failed to handle conversational response for upload_id: {upload_id}: {str(e)}", exc_info=True)
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
        
        # Keep only last 8 exchanges
        if len(self.conversation_history[upload_id]) > 20:
            self.conversation_history[upload_id] = self.conversation_history[upload_id][-8:]
