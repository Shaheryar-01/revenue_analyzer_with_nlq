# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from datetime import datetime
import logging
import sys

from services.schema_detector import PythonSchemaDetector
from services.file_manager import FileManager
from services.ai_code_agent import AICodeAgent
from services.conversation_agent import ConversationAgent
from services.code_executor import SafeCodeExecutor
from models.schemas import UploadResponse, ChatMessage, ChatResponse, FileInfo
from config.settings import get_settings

from services.context_resolver import ContextResolver


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

settings = get_settings()
logger.info("Starting AI Revenue Analyzer application")

# Initialize FastAPI
app = FastAPI(
    title="AI Revenue Analyzer",
    description="AI-powered data analysis with natural conversation",
    version="1.0.0"
)

# CORS for Next.js
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
logger.info(f"CORS configured with origins: {settings.cors_origins_list}")

# Initialize services
try:
    schema_detector = PythonSchemaDetector()
    logger.info("Schema detector initialized successfully")
    
    file_manager = FileManager()
    logger.info("File manager initialized successfully")
    
    ai_code_agent = AICodeAgent()
    logger.info("AI code agent initialized successfully")
    
    conversation_agent = ConversationAgent()
    logger.info("Conversation agent initialized successfully")
    
    context_resolver = ContextResolver()
    logger.info("Context resolver initialized successfully")



    code_executor = SafeCodeExecutor()
    logger.info("Code executor initialized successfully")
    
except Exception as e:
    logger.error(f"Failed to initialize services: {str(e)}")
    raise

@app.post("/webhook/upload", response_model=UploadResponse)
async def upload_file_webhook(file: UploadFile = File(...)):
    """
    Webhook for file upload from Next.js frontend
    Handles file upload and schema detection
    """
    logger.info(f"Upload request received for file: {file.filename}")
    
    try:
        # Validate file type
        logger.info(f"Validating file type for: {file.filename}")
        if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
            logger.warning(f"Invalid file type uploaded: {file.filename}")
            raise HTTPException(
                status_code=400, 
                detail="Only Excel (.xlsx, .xls) and CSV files are supported"
            )
        
        # Save uploaded file
        logger.info(f"Saving uploaded file: {file.filename}")
        upload_id, file_path = file_manager.save_uploaded_file(file)
        logger.info(f"File saved successfully with upload_id: {upload_id}, path: {file_path}")
        
        # Load data and detect schema using Python (no AI)
        logger.info(f"Loading dataframe for upload_id: {upload_id}")
        df = file_manager.load_dataframe(file_path)
        logger.info(f"Dataframe loaded successfully. Shape: {df.shape}")
        
        logger.info(f"Starting schema detection for upload_id: {upload_id}")
        schema_info = schema_detector.detect_complete_schema(df)
        logger.info(f"Schema detection completed for upload_id: {upload_id}")
        
        # Save schema
        logger.info(f"Saving schema for upload_id: {upload_id}")
        file_manager.save_schema(upload_id, schema_info)
        logger.info(f"Schema saved successfully for upload_id: {upload_id}")
        
        response_data = UploadResponse(
            success=True,
            upload_id=upload_id,
            filename=file.filename,
            schema_info={
                'basic_info': schema_info['basic_info'],
                'data_types': schema_info['data_types'],
                'business_entities': schema_info['business_entities'],
                'analysis_suggestions': schema_info['analysis_suggestions']
            },
            ready_for_queries=True,
            message=f"File uploaded successfully! Found {schema_info['basic_info']['total_rows']} rows and {schema_info['basic_info']['total_columns']} columns. You can now ask questions about your data."
        )
        
        logger.info(f"Upload completed successfully for upload_id: {upload_id}")
        return response_data
        
    except Exception as e:
        logger.error(f"Upload failed for file {file.filename}: {str(e)}", exc_info=True)
        return UploadResponse(
            success=False,
            upload_id="",
            filename=file.filename if file else "unknown",
            schema_info={},
            ready_for_queries=False,
            message="",
            error=str(e)
        )

@app.post("/webhook/chat/{upload_id}", response_model=ChatResponse)
async def chat_webhook(upload_id: str, message: ChatMessage):
    """
    Webhook for chat interface from Next.js frontend
    Handles natural conversation and data analysis
    """
    logger.info(f"Chat request received for upload_id: {upload_id}, message: {message.message}")
    
    try:
        # Get schema and file info
        logger.info(f"Retrieving schema for upload_id: {upload_id}")
        schema = file_manager.get_schema(upload_id)
        if not schema:
            logger.error(f"Schema not found for upload_id: {upload_id}")
            raise HTTPException(status_code=404, detail="Upload not found or schema not available")
        logger.info(f"Schema retrieved successfully for upload_id: {upload_id}")
        
        # Determine user intent
        logger.info(f"Determining user intent for upload_id: {upload_id}")
        

        intent_result = conversation_agent.determine_intent(upload_id, message.message)
        logger.info(f"User intent determined: {intent_result['intent']} for upload_id: {upload_id}")
        
        if intent_result["intent"] == "NEEDS_ANALYSIS":
            # User wants new analysis
            analysis_query = intent_result["analysis_query"]
            logger.info(f"Analysis query extracted: {analysis_query} for upload_id: {upload_id}")
            
            # Generate pandas code using AI
            logger.info(f"Generating AI code for upload_id: {upload_id}")
            ai_code = ai_code_agent.generate_analysis_code(schema, analysis_query)
            logger.info(f"AI code generated successfully for upload_id: {upload_id}")
            logger.debug(f"Generated code: {ai_code}")
            
            # Load data and execute code
            logger.info(f"Loading dataframe for execution, upload_id: {upload_id}")
            file_path = file_manager.get_file_path(upload_id)
            df = file_manager.load_dataframe(file_path)
            logger.info(f"Dataframe loaded for execution. Shape: {df.shape}, upload_id: {upload_id}")
            
            logger.info(f"Executing AI-generated code for upload_id: {upload_id}")
            execution_result = code_executor.execute_code(df, ai_code)
            
            if not execution_result['success']:
                logger.error(f"Code execution failed for upload_id: {upload_id}, error: {execution_result['error']}")
                logger.error(f"Failed code: {execution_result.get('executed_code', 'No code available')}")
                
                return ChatResponse(
                    success=False,
                    response=f"I encountered an error analyzing your data: {execution_result['error']}",
                    analysis_performed=False,
                    upload_id=upload_id,
                    timestamp=datetime.now().isoformat(),
                    error=execution_result['error']
                )
            
            logger.info(f"Code execution successful for upload_id: {upload_id}")
            logger.debug(f"Execution result: {execution_result['result']}")
            
            # Generate insights using conversation agent
            logger.info(f"Generating insights for upload_id: {upload_id}")
            insights = conversation_agent.generate_insights_response(
                upload_id=upload_id,
                user_query=message.message,
                analysis_results=execution_result['result'],
                schema=schema,
                precision_mode=True
            )
            logger.info(f"Insights generated successfully for upload_id: {upload_id}")
            
            response_data = ChatResponse(
                success=True,
                response=insights,
                analysis_performed=True,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat(),
                raw_results=execution_result['result']
            )
            
            logger.info(f"Analysis chat completed successfully for upload_id: {upload_id}")
            return response_data
        
        else:
            # User wants conversation/explanation
            logger.info(f"Handling conversational response for upload_id: {upload_id}")
            conversational_response = conversation_agent.handle_conversational_response(
                upload_id=upload_id,
                user_message=message.message,
                schema=schema
            )
            logger.info(f"Conversational response generated for upload_id: {upload_id}")
            
            response_data = ChatResponse(
                success=True,
                response=conversational_response,
                analysis_performed=False,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat()
            )
            
            logger.info(f"Conversational chat completed successfully for upload_id: {upload_id}")
            return response_data
            
    except Exception as e:
        logger.error(f"Chat webhook failed for upload_id: {upload_id}: {str(e)}", exc_info=True)
        return ChatResponse(
            success=False,
            response=f"I'm sorry, I encountered an error: {str(e)}",
            analysis_performed=False,
            upload_id=upload_id,
            timestamp=datetime.now().isoformat(),
            error=str(e)
        )

@app.get("/api/upload/{upload_id}", response_model=FileInfo)
async def get_upload_info(upload_id: str):
    """Get information about an uploaded file"""
    logger.info(f"Upload info request for upload_id: {upload_id}")
    
    upload_info = file_manager.get_upload_info(upload_id)
    if not upload_info:
        logger.warning(f"Upload info not found for upload_id: {upload_id}")
        raise HTTPException(status_code=404, detail="Upload not found")
    
    logger.info(f"Upload info retrieved successfully for upload_id: {upload_id}")
    return FileInfo(**upload_info)

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    logger.info("Health check requested")
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "schema_detector": "ready",
            "file_manager": "ready",
            "ai_agents": "ready"
        }
    }

if __name__ == "__main__":
    logger.info("Starting uvicorn server")
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=settings.debug
    )