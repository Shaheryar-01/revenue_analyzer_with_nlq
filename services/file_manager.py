# app/services/file_manager.py
import os
import uuid
import sqlite3
import pandas as pd
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging

# Configure logging for this module
logger = logging.getLogger(__name__)

class FileManager:
    def __init__(self):
        self.upload_dir = "uploads"
        self.schema_dir = "schemas"
        self.db_path = "metadata.db"
        
        logger.info("Initializing File Manager")
        
        # Create directories
        os.makedirs(self.upload_dir, exist_ok=True)
        os.makedirs(self.schema_dir, exist_ok=True)
        logger.info(f"Created/verified directories: {self.upload_dir}, {self.schema_dir}")
        
        # Initialize database
        self._init_database()
        logger.info("File Manager initialized successfully")
    
    def _init_database(self):
        """Initialize SQLite database"""
        logger.info(f"Initializing SQLite database at: {self.db_path}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_uploads (
                    upload_id TEXT PRIMARY KEY,
                    original_filename TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    schema_path TEXT,
                    file_size INTEGER,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    schema_detected_at TIMESTAMP,
                    status TEXT DEFAULT 'uploaded'
                )
            """)
            
            conn.commit()
            conn.close()
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise
    
    def save_uploaded_file(self, file) -> tuple[str, str]:
        """Save uploaded file and return upload_id and file_path"""
        logger.info(f"Saving uploaded file: {file.filename}")
        
        try:
            upload_id = str(uuid.uuid4())
            file_extension = file.filename.split('.')[-1].lower()
            file_path = f"{self.upload_dir}/{upload_id}.{file_extension}"
            
            logger.info(f"Generated upload_id: {upload_id}, file_path: {file_path}")
            
            # Save file
            logger.info("Reading file content")
            file_content = file.file.read()
            logger.info(f"Read {len(file_content)} bytes from uploaded file")
            
            with open(file_path, "wb") as f:
                f.write(file_content)
            logger.info(f"File written to disk: {file_path}")
            
            # Save to database
            logger.info("Saving file metadata to database")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO file_uploads (upload_id, original_filename, file_path, file_size)
                VALUES (?, ?, ?, ?)
            """, (upload_id, file.filename, file_path, len(file_content)))
            
            conn.commit()
            conn.close()
            logger.info(f"File metadata saved to database for upload_id: {upload_id}")
            
            return upload_id, file_path
            
        except Exception as e:
            logger.error(f"Failed to save uploaded file: {str(e)}", exc_info=True)
            raise
    
    def save_schema(self, upload_id: str, schema: Dict[str, Any]):
        """Save schema information"""
        logger.info(f"Saving schema for upload_id: {upload_id}")
        
        try:
            schema_path = f"{self.schema_dir}/{upload_id}_schema.json"
            logger.info(f"Schema file path: {schema_path}")
            
            # Save schema to file
            logger.info("Writing schema to JSON file")
            with open(schema_path, 'w') as f:
                json.dump(schema, f, indent=2, default=str)
            logger.info(f"Schema written to file: {schema_path}")
            
            # Update database
            logger.info("Updating database with schema information")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE file_uploads 
                SET schema_path = ?, schema_detected_at = CURRENT_TIMESTAMP, status = 'ready'
                WHERE upload_id = ?
            """, (schema_path, upload_id))
            
            conn.commit()
            conn.close()
            logger.info(f"Database updated with schema info for upload_id: {upload_id}")
            
        except Exception as e:
            logger.error(f"Failed to save schema for upload_id: {upload_id}: {str(e)}", exc_info=True)
            raise
    
    def get_schema(self, upload_id: str) -> Optional[Dict[str, Any]]:
        """Load schema information"""
        logger.info(f"Loading schema for upload_id: {upload_id}")
        
        try:
            schema_path = f"{self.schema_dir}/{upload_id}_schema.json"
            
            if os.path.exists(schema_path):
                logger.info(f"Schema file found: {schema_path}")
                with open(schema_path, 'r') as f:
                    schema = json.load(f)
                logger.info(f"Schema loaded successfully for upload_id: {upload_id}")
                return schema
            else:
                logger.warning(f"Schema file not found for upload_id: {upload_id}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to load schema for upload_id: {upload_id}: {str(e)}", exc_info=True)
            return None
    
    def get_file_path(self, upload_id: str) -> Optional[str]:
        """Get file path for upload_id"""
        logger.info(f"Getting file path for upload_id: {upload_id}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT file_path FROM file_uploads WHERE upload_id = ?", (upload_id,))
            result = cursor.fetchone()
            
            conn.close()
            
            if result:
                file_path = result[0]
                logger.info(f"File path found for upload_id: {upload_id} -> {file_path}")
                return file_path
            else:
                logger.warning(f"No file path found for upload_id: {upload_id}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get file path for upload_id: {upload_id}: {str(e)}", exc_info=True)
            return None
    
    def load_dataframe(self, file_path: str) -> pd.DataFrame:
        """Load file into pandas DataFrame"""
        logger.info(f"Loading dataframe from file: {file_path}")
        
        try:
            if not os.path.exists(file_path):
                logger.error(f"File does not exist: {file_path}")
                raise FileNotFoundError(f"File not found: {file_path}")
            
            if file_path.endswith(('.xlsx', '.xls')):
                logger.info("Loading Excel file")
                df = pd.read_excel(file_path)
            elif file_path.endswith('.csv'):
                logger.info("Loading CSV file")
                df = pd.read_csv(file_path)
            else:
                logger.error(f"Unsupported file type: {file_path}")
                raise ValueError(f"Unsupported file type: {file_path}")
            
            logger.info(f"Dataframe loaded successfully, shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load dataframe from {file_path}: {str(e)}", exc_info=True)
            raise
    
    def get_upload_info(self, upload_id: str) -> Optional[Dict]:
        """Get upload information"""
        logger.info(f"Getting upload info for upload_id: {upload_id}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT upload_id, original_filename, uploaded_at, status, file_size
                FROM file_uploads 
                WHERE upload_id = ?
            """, (upload_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                schema_available = os.path.exists(f"{self.schema_dir}/{upload_id}_schema.json")
                upload_info = {
                    'upload_id': result[0],
                    'filename': result[1],
                    'uploaded_at': result[2],
                    'status': result[3],
                    'file_size': result[4],
                    'schema_available': schema_available
                }
                logger.info(f"Upload info retrieved for upload_id: {upload_id}")
                return upload_info
            else:
                logger.warning(f"No upload info found for upload_id: {upload_id}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get upload info for upload_id: {upload_id}: {str(e)}", exc_info=True)
            return None
        
        
    def delete_upload(self, upload_id: str) -> bool:
        """Delete all files and database records for an upload"""
        logger.info(f"Deleting upload_id: {upload_id}")
        
        try:
            # Get file info first
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT file_path, schema_path FROM file_uploads WHERE upload_id = ?",
                (upload_id,)
            )
            result = cursor.fetchone()
            
            if not result:
                conn.close()
                logger.warning(f"No database record found for upload_id: {upload_id}")
                return False
            
            file_path, schema_path = result
            
            # Delete physical files
            files_to_delete = [
                file_path,  # Original uploaded file
                schema_path,  # Schema JSON
                f"{self.schema_dir}/{upload_id}_sheets.pkl"  # Pickled sheets
            ]
            
            for file in files_to_delete:
                if file and os.path.exists(file):
                    try:
                        os.remove(file)
                        logger.info(f"Deleted file: {file}")
                    except Exception as e:
                        logger.error(f"Failed to delete file {file}: {str(e)}")
            
            # Delete database record
            cursor.execute("DELETE FROM file_uploads WHERE upload_id = ?", (upload_id,))
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully deleted all data for upload_id: {upload_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete upload_id: {upload_id}: {str(e)}", exc_info=True)
            return False




    
        
    def load_all_sheets(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """Load all sheets from Excel file into dictionary of DataFrames"""
        logger.info(f"Loading all sheets from file: {file_path}")
        
        try:
            if not os.path.exists(file_path):
                logger.error(f"File does not exist: {file_path}")
                raise FileNotFoundError(f"File not found: {file_path}")
            
            if file_path.endswith(('.xlsx', '.xls')):
                logger.info("Loading all Excel sheets")
                # Load all sheets at once
                sheets_dict = pd.read_excel(file_path, sheet_name=None)
                logger.info(f"Loaded {len(sheets_dict)} sheets: {list(sheets_dict.keys())}")
                return sheets_dict
            elif file_path.endswith('.csv'):
                logger.info("Loading CSV file (single sheet)")
                # CSV is single sheet - wrap in dict
                df = pd.read_csv(file_path)
                return {'Sheet1': df}
            else:
                logger.error(f"Unsupported file type: {file_path}")
                raise ValueError(f"Unsupported file type: {file_path}")
                
        except Exception as e:
            logger.error(f"Failed to load sheets from {file_path}: {str(e)}", exc_info=True)
            raise

    def save_sheets_data(self, upload_id: str, sheets_dict: Dict[str, pd.DataFrame]):
        """Save all sheets data for later use"""
        logger.info(f"Saving sheets data for upload_id: {upload_id}")
        
        try:
            sheets_path = f"{self.schema_dir}/{upload_id}_sheets.pkl"
            
            # Save using pickle for efficient storage
            import pickle
            with open(sheets_path, 'wb') as f:
                pickle.dump(sheets_dict, f)
            
            logger.info(f"Sheets data saved to: {sheets_path}")
            
        except Exception as e:
            logger.error(f"Failed to save sheets data: {str(e)}", exc_info=True)
            raise

    def load_sheets_data(self, upload_id: str) -> Dict[str, pd.DataFrame]:
        """Load saved sheets data"""
        logger.info(f"Loading sheets data for upload_id: {upload_id}")
        
        try:
            sheets_path = f"{self.schema_dir}/{upload_id}_sheets.pkl"
            
            if os.path.exists(sheets_path):
                import pickle
                with open(sheets_path, 'rb') as f:
                    sheets_dict = pickle.load(f)
                logger.info(f"Loaded {len(sheets_dict)} sheets from disk")
                return sheets_dict
            else:
                logger.warning(f"Sheets data not found, loading from original file")
                file_path = self.get_file_path(upload_id)
                return self.load_all_sheets(file_path)
                
        except Exception as e:
            logger.error(f"Failed to load sheets data: {str(e)}", exc_info=True)
            raise