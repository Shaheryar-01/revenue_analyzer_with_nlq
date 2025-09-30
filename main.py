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
    """Webhook for multi-sheet file upload"""
    logger.info(f"Upload request received for file: {file.filename}")
    
    try:
        # Validate file type
        if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
            raise HTTPException(
                status_code=400, 
                detail="Only Excel (.xlsx, .xls) and CSV files are supported"
            )
        
        # Save uploaded file
        upload_id, file_path = file_manager.save_uploaded_file(file)
        logger.info(f"File saved with upload_id: {upload_id}")
        
        # Load ALL sheets
        sheets_dict = file_manager.load_all_sheets(file_path)
        logger.info(f"Loaded {len(sheets_dict)} sheets: {list(sheets_dict.keys())}")
        
        # Save sheets data for later use
        file_manager.save_sheets_data(upload_id, sheets_dict)
        
        # Detect multi-sheet schema
        if len(sheets_dict) > 1:
            schema_info = schema_detector.detect_multi_sheet_schema(sheets_dict)
            total_rows = sum(df.shape[0] for df in sheets_dict.values())
            total_cols = len(schema_info['all_columns'])
            message = f"File uploaded successfully! Found {len(sheets_dict)} sheets with {total_rows} total rows. You can now ask questions about your data."
        else:
            # Single sheet
            df = list(sheets_dict.values())[0]
            schema_info = schema_detector.detect_complete_schema(df)
            total_rows = schema_info['basic_info']['total_rows']
            total_cols = schema_info['basic_info']['total_columns']
            message = f"File uploaded successfully! Found {total_rows} rows and {total_cols} columns."
        
        # Save schema
        file_manager.save_schema(upload_id, schema_info)
        
        response_data = UploadResponse(
            success=True,
            upload_id=upload_id,
            filename=file.filename,
            schema_info={
                'sheet_count': len(sheets_dict),
                'sheet_names': list(sheets_dict.keys()),
                'total_rows': total_rows,
                'total_columns': total_cols
            },
            ready_for_queries=True,
            message=message
        )
        
        return response_data
        
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}", exc_info=True)
        return UploadResponse(
            success=False,
            upload_id="",
            filename=file.filename,
            schema_info={},
            ready_for_queries=False,
            message="",
            error=str(e)
        )
    



@app.post("/webhook/chat/{upload_id}", response_model=ChatResponse)
async def chat_webhook(upload_id: str, message: ChatMessage):
    """Webhook for multi-sheet chat interface"""
    logger.info(f"Chat request for upload_id: {upload_id}")
    
    try:
        schema = file_manager.get_schema(upload_id)
        if not schema:
            raise HTTPException(status_code=404, detail="Upload not found")
        
        import asyncio
        
        try:
            # Increased timeout to 90 seconds
            intent_result = await asyncio.wait_for(
                asyncio.to_thread(conversation_agent.determine_intent, upload_id, message.message),
                timeout=90.0
            )
        except asyncio.TimeoutError:
            logger.error(f"Intent determination timed out for upload_id: {upload_id}")
            return ChatResponse(
                success=False,
                response="Sorry, that request took too long. Please try asking a simpler question.",
                analysis_performed=False,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat(),
                error="Request timeout"
            )
        
        if intent_result["intent"] == "NEEDS_ANALYSIS":
            try:
                ai_result = await asyncio.wait_for(
                    asyncio.to_thread(ai_code_agent.generate_analysis_code, schema, intent_result["analysis_query"]),
                    timeout=90.0
                )
            except asyncio.TimeoutError:
                logger.error(f"Code generation timed out for upload_id: {upload_id}")
                return ChatResponse(
                    success=False,
                    response="The analysis is taking too long. Please try a simpler query.",
                    analysis_performed=False,
                    upload_id=upload_id,
                    timestamp=datetime.now().isoformat(),
                    error="Analysis timeout"
                )
            
            if not ai_result.get('can_answer', True):
                return ChatResponse(
                    success=True,
                    response=ai_result.get('explanation', 'Unable to process query'),
                    analysis_performed=False,
                    upload_id=upload_id,
                    timestamp=datetime.now().isoformat()
                )
            
            # Load data and execute
            is_multi_sheet = schema.get('sheet_count', 1) > 1
            
            if is_multi_sheet:
                sheets_dict = file_manager.load_sheets_data(upload_id)
                execution_result = code_executor.execute_code(
                    sheets_dict, 
                    ai_result['code'],
                    target_sheet=ai_result.get('target_sheet')
                )
            else:
                file_path = file_manager.get_file_path(upload_id)
                df = file_manager.load_dataframe(file_path)
                execution_result = code_executor.execute_code(df, ai_result['code'])
            
            if not execution_result['success']:
                return ChatResponse(
                    success=False,
                    response=f"Error analyzing data: {execution_result['error']}",
                    analysis_performed=False,
                    upload_id=upload_id,
                    timestamp=datetime.now().isoformat(),
                    error=execution_result['error']
                )
            
            try:
                insights = await asyncio.wait_for(
                    asyncio.to_thread(
                        conversation_agent.generate_insights_response,
                        upload_id,
                        message.message,
                        execution_result['result'],
                        schema,
                        True
                    ),
                    timeout=90.0
                )
            except asyncio.TimeoutError:
                return ChatResponse(
                    success=True,
                    response=f"Analysis complete: {execution_result['result']}",
                    analysis_performed=True,
                    upload_id=upload_id,
                    timestamp=datetime.now().isoformat(),
                    raw_results=execution_result['result']
                )
            
            return ChatResponse(
                success=True,
                response=insights,
                analysis_performed=True,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat(),
                raw_results=execution_result['result']
            )
        
        else:
            # Conversational response
            try:
                conversational_response = await asyncio.wait_for(
                    asyncio.to_thread(
                        conversation_agent.handle_conversational_response,
                        upload_id,
                        message.message,
                        schema
                    ),
                    timeout=90.0  # Increased timeout
                )
            except asyncio.TimeoutError:
                return ChatResponse(
                    success=False,
                    response="That question is taking too long to answer. Could you ask something more specific?",
                    analysis_performed=False,
                    upload_id=upload_id,
                    timestamp=datetime.now().isoformat(),
                    error="Response timeout"
                )
            
            return ChatResponse(
                success=True,
                response=conversational_response,
                analysis_performed=False,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat()
            )
            
    except Exception as e:
        logger.error(f"Chat failed: {str(e)}", exc_info=True)
        return ChatResponse(
            success=False,
            response=f"Error: {str(e)}",
            analysis_performed=False,
            upload_id=upload_id,
            timestamp=datetime.now().isoformat(),
            error=str(e)
        )



@app.delete("/api/upload/{upload_id}")
async def delete_upload(upload_id: str):
    """Delete an uploaded file and all associated data"""
    logger.info(f"Delete request received for upload_id: {upload_id}")
    
    try:
        # Delete from file manager (this will handle all file cleanup)
        success = file_manager.delete_upload(upload_id)
        
        if success:
            # Clear conversation history for this upload
            if upload_id in conversation_agent.conversation_history:
                del conversation_agent.conversation_history[upload_id]
                logger.info(f"Cleared conversation history for upload_id: {upload_id}")
            
            logger.info(f"Successfully deleted upload_id: {upload_id}")
            return {
                "success": True,
                "message": "File deleted successfully",
                "upload_id": upload_id
            }
        else:
            logger.warning(f"Upload not found for deletion: {upload_id}")
            raise HTTPException(status_code=404, detail="Upload not found")
            
    except Exception as e:
        logger.error(f"Failed to delete upload_id: {upload_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")
    


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