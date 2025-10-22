# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from datetime import datetime
import logging
import sys
import uuid
import os
import asyncio

from services.supabase_client import SupabaseManager
from services.excel_transformer import ExcelTransformer
from services.sql_generator import SQLGenerator
from services.query_executor import QueryExecutor
from services.conversation_agent import ConversationAgent
from models.schemas import UploadResponse, ChatMessage, ChatResponse, FileInfo
from config.settings import get_settings

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
logger.info("Starting AI Revenue Analyzer with Supabase v2.1 - Entity Recognition Enabled")

# Initialize FastAPI
app = FastAPI(
    title="AI Revenue Analyzer",
    description="AI-powered data analysis with Supabase and Entity Recognition",
    version="2.1.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
try:
    supabase_manager = SupabaseManager()
    excel_transformer = ExcelTransformer()
    sql_generator = SQLGenerator()
    query_executor = QueryExecutor()
    conversation_agent = ConversationAgent()
    
    logger.info("All services initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize services: {str(e)}")
    raise

# Temporary directory for file uploads
TEMP_DIR = "temp_uploads"
os.makedirs(TEMP_DIR, exist_ok=True)

@app.post("/webhook/upload", response_model=UploadResponse)
async def upload_file_webhook(file: UploadFile = File(...)):
    """Upload Excel file and save to Supabase database with entity extraction"""
    logger.info(f"Upload request received: {file.filename}")
    
    try:
        # Validate file type
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400,
                detail="Only Excel files (.xlsx, .xls) are supported"
            )
        
        # Generate unique upload ID
        upload_id = str(uuid.uuid4())
        logger.info(f"Generated upload_id: {upload_id}")
        
        # Save file temporarily
        temp_path = os.path.join(TEMP_DIR, f"{upload_id}_{file.filename}")
        with open(temp_path, "wb") as f:
            content = await file.read()
            f.write(content)
        logger.info(f"File saved temporarily: {temp_path}")
        
        # Validate file structure
        validation = excel_transformer.validate_excel_structure(temp_path)
        if not validation['valid']:
            os.remove(temp_path)
            raise HTTPException(status_code=400, detail=validation['message'])
        
        # Transform Excel to database records
        logger.info("Transforming Excel to database format...")
        records = excel_transformer.transform_excel_to_records(temp_path, upload_id)
        logger.info(f"Generated {len(records)} database records")
        
        # Insert into Supabase
        logger.info("Inserting data into Supabase...")
        insert_result = supabase_manager.insert_revenue_data(records)
        
        if not insert_result['success']:
            os.remove(temp_path)
            raise HTTPException(status_code=500, detail=insert_result['error'])
        
        # 🆕 STEP: Extract entity metadata for intelligent querying
        logger.info("=" * 80)
        logger.info("EXTRACTING ENTITY METADATA")
        logger.info("=" * 80)
        
        entity_metadata = excel_transformer.extract_entity_metadata(records)
        
        logger.info(f" Entity extraction complete:")
        logger.info(f"    {len(entity_metadata.get('units', []))} business units")
        logger.info(f"    {len(entity_metadata.get('regions', []))} regions")
        logger.info(f"    {len(entity_metadata.get('customers', []))} customers")
        logger.info(f"    {len(entity_metadata.get('categories', []))} categories")
        
        # 🆕 STEP: Save entity metadata to database
        supabase_manager.save_entity_metadata(upload_id, entity_metadata)
        
        # Save upload metadata
        total_rows = len(records) // 12  # Divide by 12 months to get original row count
        supabase_manager.save_upload_metadata(upload_id, file.filename, total_rows)
        
        # Clean up temp file
        os.remove(temp_path)
        logger.info(f"Temp file removed: {temp_path}")
        
        message = f"File uploaded successfully! Loaded {total_rows} projects with 12 months of data ({len(records)} total records) into database. Entity recognition enabled."
        
        return UploadResponse(
            success=True,
            upload_id=upload_id,
            filename=file.filename,
            schema_info={
                'sheet_count': 1,
                'sheet_names': ['Monthly Tracker'],
                'total_rows': total_rows,
                'total_records': len(records),
                'entities_extracted': {
                    'units': len(entity_metadata.get('units', [])),
                    'regions': len(entity_metadata.get('regions', [])),
                    'customers': len(entity_metadata.get('customers', [])),
                    'categories': len(entity_metadata.get('categories', []))
                }
            },
            ready_for_queries=True,
            message=message
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}", exc_info=True)
        # Clean up temp file if exists
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.remove(temp_path)
        
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
    """Chat interface with SQL generation and entity recognition"""
    logger.info(f"Chat request for upload_id: {upload_id}")
    logger.info(f"User message: {message.message}")
    
    try:
        # Verify upload exists
        upload_info = supabase_manager.get_upload_info(upload_id)
        if not upload_info:
            raise HTTPException(status_code=404, detail="Upload not found")
        
        # STEP 1: Determine Intent
        logger.info("=" * 80)
        logger.info("STEP 1: DETERMINING INTENT")
        logger.info("=" * 80)
        
        try:
            intent_result = await asyncio.wait_for(
                asyncio.to_thread(
                    conversation_agent.determine_intent_sql,
                    upload_id,
                    message.message
                ),
                timeout=90.0
            )
            logger.info(f"Intent: {intent_result['intent']}")
        except asyncio.TimeoutError:
            return ChatResponse(
                success=False,
                response="Request timed out. Please try a simpler question.",
                analysis_performed=False,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat(),
                error="Timeout"
            )
        
        # STEP 2: Handle OUT_OF_SCOPE
        if intent_result["intent"] == "OUT_OF_SCOPE":
            return ChatResponse(
                success=True,
                response=f"I can only answer questions about your uploaded revenue data. {intent_result.get('reason', '')}",
                analysis_performed=False,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat()
            )
        
        # STEP 3: Generate SQL for NEEDS_ANALYSIS
        if intent_result["intent"] == "NEEDS_ANALYSIS":
            # 🆕 STEP 1.5: Fetch entity metadata for intelligent SQL generation
            logger.info("=" * 80)
            logger.info("STEP 1.5: FETCHING ENTITY METADATA")
            logger.info("=" * 80)
            
            entity_metadata = supabase_manager.get_entity_metadata(upload_id)
            
            if entity_metadata:
                logger.info(f" Entity metadata loaded: {len(entity_metadata.get('units', []))} units, {len(entity_metadata.get('regions', []))} regions")
            else:
                logger.warning("  No entity metadata found - proceeding without entity recognition")
            
            logger.info("=" * 80)
            logger.info("STEP 2: GENERATING SQL WITH ENTITY AWARENESS")
            logger.info("=" * 80)
            
            # 🆕 Pass entity_metadata to SQL generator
            sql_result = sql_generator.generate_sql(
                intent_result["analysis_query"],
                upload_id,
                entity_metadata=entity_metadata  # 🆕 NEW PARAMETER
            )
            
            if not sql_result.get('can_answer'):
                return ChatResponse(
                    success=True,
                    response=sql_result.get('explanation', 'Cannot answer this query'),
                    analysis_performed=False,
                    upload_id=upload_id,
                    timestamp=datetime.now().isoformat()
                )
            
            # STEP 4: Execute SQL
            logger.info("=" * 80)
            logger.info("STEP 3: EXECUTING SQL")
            logger.info("=" * 80)
            
            execution_result = query_executor.execute_sql(
                sql_result['sql'],
                upload_id
            )
            
            if not execution_result['success']:
                return ChatResponse(
                    success=False,
                    response=f"Error executing query: {execution_result['error']}",
                    analysis_performed=False,
                    upload_id=upload_id,
                    timestamp=datetime.now().isoformat(),
                    error=execution_result['error']
                )
            
            # 🆕 STEP 3.5: Validate result quality
            logger.info("=" * 80)
            logger.info("STEP 3.5: VALIDATING RESULT QUALITY")
            logger.info("=" * 80)
            
            validation = query_executor.validate_result_quality(
                execution_result['result'],
                sql_result.get('metadata', {}),
                upload_id
            )
            
            logger.info(f"Validation result: {validation}")
            
            # STEP 5: Generate Natural Language Response
            logger.info("=" * 80)
            logger.info("STEP 4: GENERATING NATURAL LANGUAGE RESPONSE")
            logger.info("=" * 80)
            
            # 🆕 Pass validation to response generator
            insights = await asyncio.wait_for(
                asyncio.to_thread(
                    conversation_agent.generate_insights_from_sql,
                    upload_id,
                    message.message,
                    execution_result['result'],
                    sql_result.get('metadata', {}),
                    validation=validation  # 🆕 NEW PARAMETER
                ),
                timeout=90.0
            )
            
            return ChatResponse(
                success=True,
                response=insights,
                analysis_performed=True,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat(),
                raw_results=execution_result['result']
            )
        
        # STEP 6: Handle CONVERSATIONAL
        else:
            logger.info("=" * 80)
            logger.info("HANDLING CONVERSATIONAL RESPONSE")
            logger.info("=" * 80)
            
            conversational_response = await asyncio.wait_for(
                asyncio.to_thread(
                    conversation_agent.handle_conversational_sql,
                    upload_id,
                    message.message
                ),
                timeout=90.0
            )
            
            return ChatResponse(
                success=True,
                response=conversational_response,
                analysis_performed=False,
                upload_id=upload_id,
                timestamp=datetime.now().isoformat()
            )
            
    except HTTPException:
        raise
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
    """Delete upload data from database"""
    logger.info(f"Delete request for upload_id: {upload_id}")
    
    try:
        success = supabase_manager.delete_upload(upload_id)
        
        if success:
            # Clear conversation history
            if upload_id in conversation_agent.conversation_history:
                del conversation_agent.conversation_history[upload_id]
            
            return {
                "success": True,
                "message": "Upload deleted successfully",
                "upload_id": upload_id
            }
        else:
            raise HTTPException(status_code=404, detail="Upload not found")
            
    except Exception as e:
        logger.error(f"Delete failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/upload/{upload_id}", response_model=FileInfo)
async def get_upload_info(upload_id: str):
    """Get upload information"""
    logger.info(f"Info request for upload_id: {upload_id}")
    
    upload_info = supabase_manager.get_upload_info(upload_id)
    if not upload_info:
        raise HTTPException(status_code=404, detail="Upload not found")
    
    return FileInfo(
        upload_id=upload_info['upload_id'],
        filename=upload_info['filename'],
        uploaded_at=upload_info['uploaded_at'],
        status=upload_info['status'],
        file_size=upload_info.get('total_rows', 0) * 1000,  # Estimate
        schema_available=True
    )

@app.get("/api/health")
async def health_check():
    """Health check"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "2.1.0",
        "database": "supabase",
        "features": ["entity_recognition", "result_validation", "yoy_queries"]
    }

@app.delete("/api/cleanup")
async def cleanup_all_uploads():
    """
    Deletes all records from Supabase tables for a complete reset.
    """
    try:
        logger.info(" Received full cleanup request – deleting all records from Supabase tables")

        # ✅ Delete all rows from actual data tables
        supabase_manager.supabase.table("revenue_tracker").delete().not_.is_("id", None).execute()
        supabase_manager.supabase.table("upload_metadata").delete().gt("upload_id", "").execute()

        logger.info(" All real tables cleared successfully")
        return {"success": True, "message": "All tables cleared successfully"}

    except Exception as e:
        logger.error(f" Failed to clear all tables: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=settings.debug)