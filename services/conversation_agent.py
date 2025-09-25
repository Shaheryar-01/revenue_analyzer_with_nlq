# app/services/conversation_agent.py
from openai import OpenAI
import json
from config.settings import get_settings
from typing import Dict, Any, List, Optional
from datetime import datetime
import os
import logging

# Configure logging for this module
logger = logging.getLogger(__name__)

# Clear proxy environment variables
proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']
for var in proxy_vars:
    if var in os.environ:
        logger.info(f"Removing proxy environment variable: {var}")
        del os.environ[var]

settings = get_settings()
logger.info("Conversation Agent initializing...")

class ConversationAgent:
    """AI agent for natural conversation and insights generation"""
    
    def __init__(self):
        try:
            self.client = OpenAI(api_key=settings.openai_api_key)
            self.conversation_history = {}  # Store by upload_id
            logger.info("Conversation Agent initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Conversation Agent: {str(e)}")
            raise
    
    def determine_intent(self, upload_id: str, user_message: str) -> Dict[str, Any]:
        """Determine if user needs new analysis or just conversation"""
        logger.info(f"Determining intent for upload_id: {upload_id}, message: {user_message}")
        
        try:
            history = self.get_conversation_history(upload_id)
            logger.info(f"Retrieved conversation history, length: {len(history)}")
            
            prompt = f"""
            You are analyzing a user's message in a data conversation context.
            
            CONVERSATION HISTORY: {history[-3:] if history else "No previous conversation"}
            USER MESSAGE: "{user_message}"
            
            Determine the user's intent:
            1. NEEDS_ANALYSIS: User is asking for new data analysis/calculations
            2. CONVERSATIONAL: User wants explanation, discussion, or clarification about previous results
            
            NEEDS_ANALYSIS examples:
            - "What's the total sales?"
            - "Show me top 5 countries"  
            - "What about Q4 data?"
            - "Sales by product category"
            
            CONVERSATIONAL examples:
            - "That's interesting, why do you think that happened?"
            - "What would you recommend?"
            - "Can you explain this more?"
            - "That makes sense"
            
            Return JSON format:
            {{"intent": "NEEDS_ANALYSIS" or "CONVERSATIONAL", "analysis_query": "refined query for analysis if needed"}}
            
            If NEEDS_ANALYSIS, provide a clear, specific analysis_query.
            If CONVERSATIONAL, set analysis_query to null.
            """
            
            logger.info(f"Sending intent determination request to OpenAI for upload_id: {upload_id}")
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            logger.info(f"Received intent determination response for upload_id: {upload_id}")
            
            raw_response = response.choices[0].message.content
            logger.debug(f"Raw intent response: {raw_response}")
            
            try:
                intent_result = json.loads(raw_response)
                logger.info(f"Successfully parsed intent: {intent_result['intent']} for upload_id: {upload_id}")
                return intent_result
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse intent JSON response: {str(e)}, raw response: {raw_response}")
                # Default to analysis if parsing fails
                return {"intent": "NEEDS_ANALYSIS", "analysis_query": user_message}
                
        except Exception as e:
            logger.error(f"Failed to determine intent for upload_id: {upload_id}: {str(e)}", exc_info=True)
            # Default to analysis if error occurs
            return {"intent": "NEEDS_ANALYSIS", "analysis_query": user_message}
    
    def generate_insights_response(
    self, 
    upload_id: str,
    user_query: str, 
    analysis_results: Any,
    schema: Dict[str, Any],
    precision_mode: bool = False
) -> str:
        """Generate insights from analysis results"""
        logger.info(f"Generating insights response for upload_id: {upload_id}")
        logger.debug(f"Analysis results type: {type(analysis_results)}, content: {analysis_results}")
        
        try:
            history = self.get_conversation_history(upload_id)
            file_info = schema.get('basic_info', {})
            logger.info(f"Retrieved context - history length: {len(history)}, file info available: {bool(file_info)}")
            
            if precision_mode:
                prompt = f"""
                You are a financial analyst. The CEO asked: "{user_query}".
                
                IMPORTANT:
                - Always present numbers exactly as returned in analysis_results
                - Show full precision (format with commas and two decimal places)
                - Do not approximate, round, or summarize into "millions"
                - Do not convert to 'approximate' values
                - If the analysis_results already contain exact numeric values (especially when the query involves "trend", "total", "units", "sales", or "revenue"), 
                  you MUST present those exact values without alteration. 
                - Do not round, approximate, or generate new numbers. 
                - If interpretation is needed, first present the exact values, then add explanation.

                
                ANALYSIS RESULTS: {analysis_results}
                
                Return the direct, exact answer in plain English.
                """
            else:
                prompt = f"""
                You are a senior business analyst presenting insights to a CEO. 
                
                CONTEXT:
                - CEO asked: "{user_query}"
                - Analysis returned: {analysis_results}
                - Dataset: {file_info.get('total_rows', 'unknown')} rows, {file_info.get('total_columns', 'unknown')} columns
                - Previous conversation: {history[-2:] if history else "None"}
                
                TASK:
                Convert the technical analysis results into clear, executive-level insights.
                
                GUIDELINES:
                1. Start with direct answer to the question
                2. Use specific numbers and percentages
                3. Highlight key business implications
                4. Maintain conversational, professional tone
                5. Reference previous conversation if relevant
                6. Suggest follow-up questions if appropriate
                """
            
            logger.info(f"Sending insights generation request to OpenAI for upload_id: {upload_id}")
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            logger.info(f"Received insights response for upload_id: {upload_id}")
            
            insights = response.choices[0].message.content
            logger.info(f"Generated insights successfully for upload_id: {upload_id}, length: {len(insights)}")
            
            # Save to conversation history
            self.add_to_history(upload_id, user_query, insights)
            
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate insights for upload_id: {upload_id}: {str(e)}", exc_info=True)
            raise




    def handle_conversational_response(
        self, 
        upload_id: str, 
        user_message: str,
        schema: Dict[str, Any]
    ) -> str:
        """Handle purely conversational messages"""
        logger.info(f"Handling conversational response for upload_id: {upload_id}")
        
        try:
            history = self.get_conversation_history(upload_id)
            logger.info(f"Retrieved conversation history, length: {len(history)}")
            
            prompt = f"""
            Continue this business conversation with a CEO about their data.
            
            CONVERSATION HISTORY: {history}
            CEO SAYS: "{user_message}"
            
            CONTEXT: You've been analyzing the CEO's data and providing insights.
            
            Respond as a knowledgeable business analyst would:
            1. Be helpful and insightful
            2. Reference previous analysis if relevant
            3. Ask clarifying questions if appropriate
            4. Provide business advice when suitable
            5. Maintain professional but conversational tone
            
            Do NOT make up new data or statistics. Use only information from previous conversation.
            """
            
            logger.info(f"Sending conversational request to OpenAI for upload_id: {upload_id}")
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.4
            )
            logger.info(f"Received conversational response for upload_id: {upload_id}")
            
            conversational_response = response.choices[0].message.content
            logger.info(f"Generated conversational response successfully for upload_id: {upload_id}, length: {len(conversational_response)}")
            
            # Save to history
            logger.info(f"Adding conversational exchange to history for upload_id: {upload_id}")
            self.add_to_history(upload_id, user_message, conversational_response)
            logger.info(f"Conversational exchange added to history for upload_id: {upload_id}")
            
            return conversational_response
            
        except Exception as e:
            logger.error(f"Failed to handle conversational response for upload_id: {upload_id}: {str(e)}", exc_info=True)
            raise
    
    def get_conversation_history(self, upload_id: str) -> List[Dict]:
        """Get conversation history"""
        logger.debug(f"Getting conversation history for upload_id: {upload_id}")
        history = self.conversation_history.get(upload_id, [])
        logger.debug(f"Retrieved {len(history)} conversation entries for upload_id: {upload_id}")
        return history
    
    def add_to_history(self, upload_id: str, query: str, response: str):
        """Add exchange to conversation history"""
        logger.info(f"Adding exchange to history for upload_id: {upload_id}")
        
        if upload_id not in self.conversation_history:
            self.conversation_history[upload_id] = []
            logger.info(f"Created new conversation history for upload_id: {upload_id}")
        
        self.conversation_history[upload_id].append({
            'user_message': query,
            'assistant_response': response,
            'timestamp': datetime.now().isoformat()
        })
        
        # Keep only last 8 exchanges to manage context
        history_length_before = len(self.conversation_history[upload_id])
        if history_length_before > 8:
            self.conversation_history[upload_id] = self.conversation_history[upload_id][-8:]
            logger.info(f"Trimmed conversation history from {history_length_before} to 8 entries for upload_id: {upload_id}")
        
        logger.info(f"Exchange added to history successfully for upload_id: {upload_id}, total entries: {len(self.conversation_history[upload_id])}")