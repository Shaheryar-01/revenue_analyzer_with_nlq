# app/services/file_manager.py
import os
import uuid
import sqlite3
import pandas as pd
import numpy as np
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
        """Save schema information with year detection from filename"""
        logger.info(f"Saving schema for upload_id: {upload_id}")
        
        try:
            schema_path = f"{self.schema_dir}/{upload_id}_schema.json"
            logger.info(f"Schema file path: {schema_path}")
            
            # ✅ EXTRACT YEAR FROM FILENAME
            upload_info = self.get_upload_info(upload_id)
            if upload_info:
                filename = upload_info.get('filename', '')
                logger.info(f"Extracting year from filename: {filename}")
                
                # Look for 4-digit year in filename (20XX pattern)
                import re
                year_match = re.search(r'(20\d{2})', filename)
                if year_match:
                    detected_year = year_match.group(1)
                    
                    # Add to column_patterns if it exists
                    if 'column_patterns' in schema:
                        schema['column_patterns']['inferred_year'] = detected_year
                        logger.info(f"✅ Inferred year {detected_year} from filename '{filename}'")
                    else:
                        # If column_patterns doesn't exist yet, add it
                        schema['column_patterns'] = {'inferred_year': detected_year}
                        logger.info(f"✅ Created column_patterns with inferred year {detected_year}")
            
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
        """Load file into pandas DataFrame with normalization and smart header detection"""
        logger.info(f"Loading dataframe from file: {file_path}")
        
        try:
            if not os.path.exists(file_path):
                logger.error(f"File does not exist: {file_path}")
                raise FileNotFoundError(f"File not found: {file_path}")
            
            if file_path.endswith(('.xlsx', '.xls')):
                logger.info("Loading Excel file")
                
                # CRITICAL FIX: Detect which row has the actual headers
                df_preview = pd.read_excel(file_path, header=None, nrows=5)
                header_row = self._detect_header_row(df_preview)
                logger.info(f"Detected header row at index: {header_row}")
                
                # Read with correct header
                if header_row > 0:
                    df = pd.read_excel(file_path, header=header_row)
                else:
                    df = pd.read_excel(file_path)
                
                # Remove empty rows
                df = df.dropna(how='all')
                
            elif file_path.endswith('.csv'):
                logger.info("Loading CSV file")
                df = pd.read_csv(file_path)
            else:
                logger.error(f"Unsupported file type: {file_path}")
                raise ValueError(f"Unsupported file type: {file_path}")
            
            logger.info(f"Dataframe loaded, shape: {df.shape}")
            logger.info(f"Columns: {list(df.columns)}")
            
            # NORMALIZE THE DATAFRAME
            df = self._normalize_dataframe(df, "single_sheet")
            logger.info(f"Dataframe normalized successfully")
            
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
                file_path,
                schema_path,
                f"{self.schema_dir}/{upload_id}_sheets.pkl"
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
        """Load ONLY the first sheet from Excel file with normalization and smart header detection"""
        logger.info(f"Loading first sheet only from file: {file_path}")
        
        try:
            if not os.path.exists(file_path):
                logger.error(f"File does not exist: {file_path}")
                raise FileNotFoundError(f"File not found: {file_path}")
            
            if file_path.endswith(('.xlsx', '.xls')):
                logger.info("Loading first Excel sheet only")
                
                # CRITICAL FIX: Detect which row has the actual headers
                # Read first few rows without header to inspect
                df_preview = pd.read_excel(file_path, sheet_name=0, header=None, nrows=5)
                logger.info(f"Preview of first 5 rows:\n{df_preview}")
                
                # Find the row with the most non-null, meaningful values
                # This is likely the header row
                header_row = self._detect_header_row(df_preview)
                logger.info(f"Detected header row at index: {header_row}")
                
                # Now read with the correct header
                if header_row > 0:
                    df = pd.read_excel(file_path, sheet_name=0, header=header_row)
                    logger.info(f"Loaded with header at row {header_row}")
                else:
                    df = pd.read_excel(file_path, sheet_name=0)
                    logger.info(f"Loaded with default header (row 0)")
                
                # Get the actual sheet name
                xl_file = pd.ExcelFile(file_path)
                first_sheet_name = xl_file.sheet_names[0]
                logger.info(f"Loaded first sheet: '{first_sheet_name}'")
                logger.info(f"Detected columns: {list(df.columns)}")
                
                # Remove rows that are all NaN (empty rows)
                df = df.dropna(how='all')
                logger.info(f"After removing empty rows, shape: {df.shape}")
                
                sheets_dict = {first_sheet_name: df}
                
            elif file_path.endswith('.csv'):
                logger.info("Loading CSV file (single sheet)")
                df = pd.read_csv(file_path)
                sheets_dict = {'Sheet1': df}
            else:
                logger.error(f"Unsupported file type: {file_path}")
                raise ValueError(f"Unsupported file type: {file_path}")
            
            # NORMALIZE THE SHEET
            normalized_sheets = {}
            for sheet_name, df in sheets_dict.items():
                logger.info(f"Normalizing sheet: {sheet_name}")
                normalized_sheets[sheet_name] = self._normalize_dataframe(df, sheet_name)
                logger.info(f"Sheet {sheet_name} normalized successfully")
            
            return normalized_sheets
                
        except Exception as e:
            logger.error(f"Failed to load sheets from {file_path}: {str(e)}", exc_info=True)
            raise

    def _detect_header_row(self, df_preview: pd.DataFrame) -> int:
        """
        Detect which row contains the actual column headers
        
        Strategy:
        1. Look for rows with most unique, non-numeric text values
        2. Avoid rows with mostly "Unnamed" or numeric values
        3. Return the row index (0-based)
        """
        logger.info("Detecting header row...")
        
        best_row = 0
        best_score = 0
        
        for idx in range(min(5, len(df_preview))):
            row = df_preview.iloc[idx]
            
            # Count non-null values
            non_null_count = row.notna().sum()
            
            # Count unique values
            unique_count = row.nunique()
            
            # Count text values (not numbers)
            text_count = sum(1 for val in row if isinstance(val, str) and len(str(val).strip()) > 0)
            
            # Penalize rows with "Unnamed" (likely not real headers)
            unnamed_count = sum(1 for val in row if isinstance(val, str) and 'Unnamed' in str(val))
            
            # Calculate score
            score = (non_null_count * 2) + (unique_count * 3) + (text_count * 4) - (unnamed_count * 10)
            
            logger.info(f"Row {idx}: non_null={non_null_count}, unique={unique_count}, text={text_count}, unnamed={unnamed_count}, score={score}")
            
            if score > best_score:
                best_score = score
                best_row = idx
        
        logger.info(f"Best header row: {best_row} with score {best_score}")
        return best_row





    def _normalize_dataframe(self, df: pd.DataFrame, sheet_name: str = "unknown") -> pd.DataFrame:
        """
        CRITICAL: Normalize data types for consistent querying
        This is the single source of truth for data cleaning
        """
        logger.info(f"=" * 80)
        logger.info(f"NORMALIZING SHEET: {sheet_name}")
        logger.info(f"Input shape: {df.shape}")
        logger.info(f"Input columns: {list(df.columns)}")
        logger.info(f"=" * 80)
        
        df = df.copy()
        
        # Track what changed
        normalization_report = []
        errors = []
        warnings = []
        
        # STEP 1: Clean column names (remove leading/trailing spaces)
        original_columns = df.columns.tolist()
        df.columns = df.columns.str.strip()
        renamed_cols = [(old, new) for old, new in zip(original_columns, df.columns) if old != new]
        if renamed_cols:
            logger.info(f"Cleaned column names: {renamed_cols}")
            normalization_report.append(f"Cleaned {len(renamed_cols)} column names")
        
        # STEP 2: Check for duplicate column names (CRITICAL ERROR)
        if len(df.columns) != len(set(df.columns)):
            duplicates = df.columns[df.columns.duplicated()].tolist()
            error_msg = f"Duplicate column names found: {duplicates}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # STEP 3: Check for empty column names (CRITICAL ERROR)
        empty_cols = [col for col in df.columns if not col or str(col).strip() == '']
        if empty_cols:
            error_msg = f"Empty column names found at positions: {[df.columns.get_loc(c) for c in empty_cols]}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # STEP 4: Normalize each column
        for col in df.columns:
            original_dtype = df[col].dtype
            col_sample = df[col].dropna().head(5).tolist()
            
            try:
                # 4A. DATE NORMALIZATION
                if self._looks_like_date_column(df[col]):
                    logger.info(f"Normalizing date column: {col}")
                    logger.debug(f"  Sample values before: {col_sample}")
                    
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    
                    # Validate: check how many failed to parse
                    null_count = df[col].isnull().sum()
                    total_count = len(df)
                    parse_success_rate = ((total_count - null_count) / total_count) * 100
                    
                    logger.info(f"  Parsed as datetime64: {parse_success_rate:.1f}% success")
                    
                    if parse_success_rate < 50:
                        warnings.append(f"Column '{col}': Only {parse_success_rate:.1f}% parsed as dates")
                    
                    normalization_report.append(f"{col}: {original_dtype} → datetime64[ns]")
                
                # 4B. NUMERIC CONVERSION (CRITICAL FIX)
                # Try to convert object columns to numeric if they contain numbers
                elif df[col].dtype == 'object':
                    logger.info(f"Checking if string column '{col}' can be numeric")
                    
                    # Try numeric conversion first
                    numeric_converted = pd.to_numeric(df[col], errors='coerce')
                    
                    # If more than 50% of non-null values successfully converted, treat as numeric
                    non_null_original = df[col].notna().sum()
                    non_null_numeric = numeric_converted.notna().sum()
                    
                    if non_null_original > 0:
                        conversion_rate = non_null_numeric / non_null_original
                        
                        if conversion_rate > 0.5:  # If >50% are numeric
                            logger.info(f"  Converting '{col}' to numeric ({conversion_rate*100:.1f}% conversion rate)")
                            df[col] = numeric_converted
                            normalization_report.append(f"{col}: {original_dtype} → numeric (auto-detected)")
                        else:
                            # It's truly a string column
                            logger.info(f"  Keeping '{col}' as string (only {conversion_rate*100:.1f}% numeric)")
                            df[col] = df[col].astype(str).str.strip()
                            
                            # Replace common null representations
                            null_values = ['nan', 'NaN', 'None', 'NULL', 'null', '', 'N/A', 'n/a', 'NA']
                            df[col] = df[col].replace(null_values, pd.NA)
                            
                            normalization_report.append(f"{col}: cleaned strings")
                    else:
                        # Column is entirely null
                        logger.warning(f"  Column '{col}' is entirely null")
                        warnings.append(f"Column '{col}': All values are null")
                
                # 4C. ALREADY NUMERIC - ensure proper type
                elif pd.api.types.is_numeric_dtype(df[col]):
                    if df[col].dtype not in ['float64', 'int64']:
                        logger.info(f"Normalizing numeric column: {col}")
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        normalization_report.append(f"{col}: {original_dtype} → numeric")
                
                # 4D. UNKNOWN TYPE
                else:
                    logger.warning(f"Column '{col}' has unhandled dtype: {original_dtype}")
                    warnings.append(f"Column '{col}': Unknown dtype {original_dtype}")
            
            except Exception as e:
                error_msg = f"Failed to normalize column '{col}': {str(e)}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)
                continue
        
        # STEP 5: Post-normalization validation
        logger.info(f"=" * 80)
        logger.info(f"VALIDATION RESULTS FOR SHEET: {sheet_name}")
        logger.info(f"=" * 80)
        
        # Log dtype distribution
        dtype_counts = df.dtypes.value_counts().to_dict()
        logger.info(f"Data type distribution: {dtype_counts}")
        
        # Log normalization summary
        logger.info(f"Normalization changes: {len(normalization_report)}")
        for change in normalization_report:
            logger.info(f"  ✓ {change}")
        
        # Log warnings
        if warnings:
            logger.warning(f"Warnings: {len(warnings)}")
            for warning in warnings:
                logger.warning(f"  ⚠️ {warning}")
        
        # Log errors (non-critical)
        if errors:
            logger.error(f"Errors encountered: {len(errors)}")
            for error in errors:
                logger.error(f"  ❌ {error}")
        
        # Check for columns that are entirely null
        null_cols = [col for col in df.columns if df[col].isnull().all()]
        if null_cols:
            logger.warning(f"Columns with all null values: {null_cols}")
        
        logger.info(f"✅ Normalization complete for '{sheet_name}'")
        logger.info(f"Final shape: {df.shape}")
        logger.info(f"=" * 80)
        
        return df





    def _looks_like_date_column(self, series: pd.Series) -> bool:
        """
        Heuristic to detect date columns with comprehensive checks
        """
        # CRITICAL: If column is already numeric, it's NOT a date
        if pd.api.types.is_numeric_dtype(series):
            logger.debug(f"Column '{series.name}' is numeric - NOT a date column")
            return False
        
        # Skip if too few non-null values
        non_null_count = series.notna().sum()
        if non_null_count == 0:
            return False
        
        # Check 1: Column name contains date keywords
        date_keywords = ['date', 'time', 'created', 'modified', 'timestamp', 'datetime', 
                        'day', 'dated']  # Removed 'year', 'month' - too generic
        column_name_lower = series.name.lower() if series.name else ''
        
        name_match = any(keyword in column_name_lower for keyword in date_keywords)
        
        # Check 2: Column dtype is already object (string-like)
        if series.dtype != 'object':
            logger.debug(f"Column '{series.name}' is not object dtype - NOT a date column")
            return False
        
        if name_match:
            logger.debug(f"Column '{series.name}' matched date keyword in name")
        
        # Check 3: Try parsing sample data
        sample_size = min(100, non_null_count)
        sample = series.dropna().head(sample_size)
        
        try:
            # Check if values look like dates (contain /, -, or are long strings)
            sample_str = sample.astype(str)
            looks_like_date_format = sample_str.str.contains(r'[/-]|^\d{4}', na=False).sum() / len(sample)
            
            if looks_like_date_format < 0.5:
                logger.debug(f"Column '{series.name}' doesn't have date-like format")
                return False
            
            # Now try parsing
            parsed = pd.to_datetime(sample, errors='coerce')
            parse_success_rate = parsed.notna().sum() / len(sample)
            
            logger.debug(f"Column '{series.name}' date parse rate: {parse_success_rate*100:.1f}%")
            
            # Decision logic:
            # - Must have name match OR >90% parse rate (very high bar)
            # - AND must look like date format
            
            if name_match and parse_success_rate > 0.7:
                return True
            elif parse_success_rate > 0.95:  # Very high threshold for non-name-matched columns
                return True
            else:
                return False
                
        except Exception as e:
            logger.debug(f"Column '{series.name}' failed date detection: {str(e)}")
            return False
        




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