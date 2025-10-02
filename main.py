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
    """Webhook for multi-sheet chat interface with OUT_OF_SCOPE detection"""
    logger.info(f"Chat request for upload_id: {upload_id}")
    logger.info(f"User message: {message.message}")
    
    try:
        schema = file_manager.get_schema(upload_id)
        if not schema:
            raise HTTPException(status_code=404, detail="Upload not found")
        
        import asyncio
        
        # ============================================================================
        # STEP 1: DETERMINE INTENT (with schema passed to detect OUT_OF_SCOPE)
        # ============================================================================
        try:
            logger.info("=" * 80)
            logger.info("STEP 1: DETERMINING INTENT")
            logger.info("=" * 80)
            
            intent_result = await asyncio.wait_for(
                asyncio.to_thread(
                    conversation_agent.determine_intent, 
                    upload_id, 
                    message.message, 
                    schema  # FIXED: Pass schema to enable OUT_OF_SCOPE detection
                ),
                timeout=90.0
            )
            
            logger.info(f"Intent Result: {intent_result}")
            
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
        
        # ============================================================================
        # STEP 2: HANDLE OUT_OF_SCOPE QUERIES
        # ============================================================================
        if intent_result["intent"] == "OUT_OF_SCOPE":
            logger.info("=" * 80)
            logger.info("QUERY IS OUT OF SCOPE - REJECTING")
            logger.info(f"Reason: {intent_result.get('reason', 'Unknown')}")
            logger.info("=" * 80)
            
            # Get column info for helpful message
            if schema.get('sheet_count', 1) > 1:
                columns_preview = schema.get('all_columns', [])[:5]
            else:
                columns_preview = schema.get('basic_info', {}).get('column_names', [])[:5]
            
            out_of_scope_message = (
                f"I can only answer questions about your uploaded data. "
                f"{intent_result.get('reason', 'Your question appears to be outside the scope of the dataset.')} "
                f"\n\n**Your dataset contains columns like:** {', '.join(columns_preview)}..."
                f"\n\nPlease ask questions about this data."
            )
            
            return ChatResponse(
                success=True,
                response=out_of_scope_message,
                analysis_performed=False,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat()
            )
        
        # ============================================================================
        # STEP 3: HANDLE NEEDS_ANALYSIS
        # ============================================================================
        if intent_result["intent"] == "NEEDS_ANALYSIS":
            logger.info("=" * 80)
            logger.info("STEP 2: GENERATING ANALYSIS CODE")
            logger.info(f"Analysis Query: {intent_result['analysis_query']}")
            logger.info("=" * 80)
            
            try:
                ai_result = await asyncio.wait_for(
                    asyncio.to_thread(
                        ai_code_agent.generate_analysis_code, 
                        schema, 
                        intent_result["analysis_query"]
                    ),
                    timeout=90.0
                )
                
                logger.info(f"AI Code Generation Result:")
                logger.info(f"  - can_answer: {ai_result.get('can_answer', True)}")
                logger.info(f"  - target_sheet: {ai_result.get('target_sheet')}")
                logger.info(f"  - code_length: {len(ai_result.get('code', '')) if ai_result.get('code') else 0}")
                
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
                logger.warning(f"AI cannot answer query: {ai_result.get('explanation', 'Unknown reason')}")
                return ChatResponse(
                    success=True,
                    response=ai_result.get('explanation', 'Unable to process query'),
                    analysis_performed=False,
                    upload_id=upload_id,
                    timestamp=datetime.now().isoformat()
                )
            
            # ========================================================================
            # STEP 4: EXECUTE CODE
            # ========================================================================
            logger.info("=" * 80)
            logger.info("STEP 3: EXECUTING CODE")
            logger.info("=" * 80)
            logger.info(f"Generated Code:\n{ai_result['code']}")
            
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
            
            # ========================================================================
            # CRITICAL LOGGING: RAW RESULTS BEFORE INSIGHTS GENERATION
            # ========================================================================
            logger.info("=" * 80)
            logger.info("STEP 4: CODE EXECUTION RESULTS (BEFORE INSIGHTS)")
            logger.info("=" * 80)
            logger.info(f"Execution Success: {execution_result['success']}")
            logger.info(f"Raw Result Type: {type(execution_result['result'])}")
            logger.info(f"Raw Result Value: {execution_result['result']}")
            logger.info(f"Raw Result Length: {len(str(execution_result['result']))}")
            
            if not execution_result['success']:
                logger.error(f"Code execution failed: {execution_result['error']}")
                return ChatResponse(
                    success=False,
                    response=f"Error analyzing data: {execution_result['error']}",
                    analysis_performed=False,
                    upload_id=upload_id,
                    timestamp=datetime.now().isoformat(),
                    error=execution_result['error']
                )
            
            # ========================================================================
            # STEP 5: GENERATE INSIGHTS (with hallucination prevention)
            # ========================================================================
            logger.info("=" * 80)
            logger.info("STEP 5: GENERATING INSIGHTS FROM RAW RESULTS")
            logger.info("=" * 80)
            
            # After execution succeeds, around line 258 in main.py:
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
                insights = f"Analysis complete: {execution_result['result']}"
            except Exception as e:
                logger.error(f"Insights generation failed: {str(e)}", exc_info=True)
                insights = f"Result: {execution_result['result']}"

            # THIS MUST ALWAYS EXECUTE
            return ChatResponse(
                success=True,
                response=insights,
                analysis_performed=True,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat(),
                raw_results=execution_result['result']
            )

        
        # ============================================================================
        # STEP 6: HANDLE CONVERSATIONAL
        # ============================================================================
        else:
            logger.info("=" * 80)
            logger.info("HANDLING CONVERSATIONAL RESPONSE")
            logger.info("=" * 80)
            
            try:
                conversational_response = await asyncio.wait_for(
                    asyncio.to_thread(
                        conversation_agent.handle_conversational_response,
                        upload_id,
                        message.message,
                        schema
                    ),
                    timeout=90.0
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
            
            logger.info(f"Conversational Response: {conversational_response}")
            
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