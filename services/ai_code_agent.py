# app/services/ai_code_agent.py
from openai import OpenAI
from config.settings import get_settings
from typing import Dict, Any
import os
import logging
import re


# Configure logging for this module
logger = logging.getLogger(__name__)

# Clear any proxy environment variables that might interfere
proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']
for var in proxy_vars:
    if var in os.environ:
        logger.info(f"Removing proxy environment variable: {var}")
        del os.environ[var]

settings = get_settings()
logger.info("AI Code Agent initializing...")

class AICodeAgent:
    """AI agent that generates pandas code for user queries"""
    
    def __init__(self):
        try:
            self.client = OpenAI(api_key=settings.openai_api_key)
            logger.info("OpenAI client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {str(e)}")
            raise
    




    def generate_analysis_code(self, schema: Dict[str, Any], user_query: str) -> str:
        """Generate pandas code based on user query and schema"""
        logger.info(f"Starting code generation for query: {user_query}")
        
        try:
            # Extract relevant schema info
            basic_info = schema.get('basic_info', {})
            columns = basic_info.get('column_names', [])
            data_types = schema.get('data_types', {})
            business_entities = schema.get('business_entities', {})
            sample_data = schema.get('sample_data', {})
            
            logger.info(f"Schema info extracted - Columns: {len(columns)}, Data types: {len(data_types)}")
            
            prompt = f"""
            You are a pandas expert. Generate Python code to answer this user query about their dataset.
            
            USER QUERY: "{user_query}"
            
            DATASET INFO:
            - Shape: {basic_info.get('shape', 'unknown')}
            - Columns: {columns}
            - Numerical columns: {data_types.get('numerical', [])}
            - Categorical columns: {data_types.get('categorical', [])}
            - Date columns: {data_types.get('datetime', [])}
            - Business entities detected: {list(business_entities.keys())}
            - Sample data: {sample_data.get('data_preview', {})}
            
            INSTRUCTIONS:
            1. Generate pandas code that directly answers the user's question
            2. Store the result in a variable called 'query_result'
            3. Handle edge cases (empty results, missing columns, etc.)
            4. Use only pandas and numpy operations
            5. Assume the dataframe is called 'df'
            6. Make the result JSON-serializable (convert to dict/list if needed)
            7. If the query contains words like "trend", "trends", "growth", "total", "units", "sales", "revenue", "profit", or "summary", 
            you MUST generate code that produces exact numeric results directly from the dataframe. 
            Do not approximate or round. Use only columns from the provided schema.


            EXAMPLES:
            - "What's the total sales?" → query_result = df['Sales'].sum()
            - "Top 5 countries by revenue?" → query_result = df.groupby('Country')['Revenue'].sum().nlargest(5).to_dict()
            - "Average profit margin?" → query_result = (df['Profit'] / df['Sales']).mean()
            - "Sales by month?" → query_result = df.groupby(df['Date'].dt.to_period('M'))['Sales'].sum().astype(str).to_dict()
            
            Return ONLY the Python code that answers the query. No explanations.
            """
            
            logger.info("Sending request to OpenAI API")
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=1000
            )
            logger.info("Received response from OpenAI API")
            
            code = response.choices[0].message.content
            logger.info(f"Raw code response received, length: {len(code)}")
            
            # Clean up code
            original_code = code
            if "```python" in code:
                code = code.split("```python")[1].split("```")[0]
                logger.info("Extracted code from python code block")
            elif "```" in code:
                code = code.split("```")[1].split("```")[0]
                logger.info("Extracted code from generic code block")
            
            code = code.strip()
            logger.info(f"Cleaned code generated successfully, length: {len(code)}")
            logger.debug(f"Generated code: {code}")
            
            return code
            
        except Exception as e:
            logger.error(f"Failed to generate analysis code: {str(e)}", exc_info=True)
            raise