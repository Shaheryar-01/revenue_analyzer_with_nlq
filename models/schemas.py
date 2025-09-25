# app/models/schemas.py
from pydantic import BaseModel
from typing import Optional, Dict, Any, Union
from datetime import datetime

class UploadResponse(BaseModel):
    success: bool
    upload_id: str
    filename: str
    schema_info: Dict[str, Any]
    ready_for_queries: bool
    message: str
    error: Optional[str] = None

class ChatMessage(BaseModel):
    message: str
    timestamp: str

class ChatResponse(BaseModel):
    success: bool
    response: str
    analysis_performed: bool
    upload_id: str
    timestamp: str
    error: Optional[str] = None
    # Changed from Dict[str, Any] to Union to accept different types
    raw_results: Optional[Union[Dict[str, Any], list, str, int, float]] = None

class FileInfo(BaseModel):
    upload_id: str
    filename: str
    uploaded_at: str
    status: str
    file_size: int
    schema_available: bool