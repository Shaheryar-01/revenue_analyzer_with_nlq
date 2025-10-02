# app/services/schema_detector.py
import pandas as pd
import numpy as np
from typing import Dict, Any, List
from datetime import datetime
import json
import logging

# Configure logging for this module
logger = logging.getLogger(__name__)

class PythonSchemaDetector:
    """Pure Python schema detection - works with normalized data"""
    
    def __init__(self):
        self.business_keywords = {
            'revenue': ['revenue', 'sales', 'amount', 'total', 'income', 'earnings'],
            'profit': ['profit', 'margin', 'net', 'gross'],
            'cost': ['cost', 'expense', 'spending'],
            'date': ['date', 'time', 'created', 'modified', 'year', 'month'],
            'geography': ['country', 'region', 'state', 'city', 'location'],
            'product': ['product', 'item', 'sku', 'category'],
            'customer': ['customer', 'client', 'user', 'account'],
            'quantity': ['quantity', 'units', 'count', 'volume']
        }
        logger.info("Python Schema Detector initialized successfully")
    
    def detect_complete_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect complete schema - assumes normalized data"""
        logger.info(f"Starting complete schema detection for dataframe with shape: {df.shape}")
        
        try:
            schema = {
                'basic_info': self._get_basic_info(df),
                'columns': self._analyze_all_columns(df),
                'data_types': self._categorize_columns(df),
                'business_entities': self._detect_business_entities(df),
                'data_quality': self._assess_data_quality(df),
                'relationships': self._detect_relationships(df),
                'sample_data': self._get_sample_data(df),
                'analysis_suggestions': self._suggest_analyses(df)
            }
            
            logger.info("Complete schema detection finished successfully")
            return schema
            
        except Exception as e:
            logger.error(f"Failed to detect complete schema: {str(e)}", exc_info=True)
            raise
    
    def _get_basic_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Basic dataframe information"""
        logger.info("Getting basic dataframe information")
        
        try:
            basic_info = {
                'shape': df.shape,
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
                'column_names': list(df.columns)
            }
            logger.info(f"Basic info extracted: {basic_info['total_rows']} rows, {basic_info['total_columns']} columns")
            return basic_info
            
        except Exception as e:
            logger.error(f"Failed to get basic info: {str(e)}")
            raise
    
    def _analyze_all_columns(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """Analyze each column individually - recognizes normalized types"""
        logger.info(f"Analyzing all columns: {len(df.columns)} columns to process")
        
        try:
            columns_analysis = {}
            
            for i, col in enumerate(df.columns):
                logger.debug(f"Analyzing column {i+1}/{len(df.columns)}: {col}")
                
                columns_analysis[col] = {
                    'dtype': str(df[col].dtype),
                    'non_null_count': int(df[col].count()),
                    'null_count': int(df[col].isnull().sum()),
                    'null_percentage': round((df[col].isnull().sum() / len(df)) * 100, 2),
                    'unique_values': int(df[col].nunique()),
                    'unique_percentage': round((df[col].nunique() / len(df)) * 100, 2),
                    'sample_values': self._get_safe_sample_values(df[col]),
                    'is_constant': df[col].nunique() <= 1,
                    'data_characteristics': self._analyze_column_characteristics(df[col]),
                    'normalized_type': self._get_normalized_type(df[col])
                }
            
            logger.info(f"Column analysis completed for all {len(df.columns)} columns")
            return columns_analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze columns: {str(e)}", exc_info=True)
            raise
    
    def _get_normalized_type(self, series: pd.Series) -> str:
        """Get the normalized data type category"""
        if pd.api.types.is_datetime64_any_dtype(series):
            return 'datetime'
        elif pd.api.types.is_numeric_dtype(series):
            return 'numeric'
        elif series.dtype == 'object' or pd.api.types.is_string_dtype(series):
            return 'string'
        elif pd.api.types.is_bool_dtype(series):
            return 'boolean'
        else:
            return 'unknown'
    
    def _categorize_columns(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Categorize columns by normalized data type"""
        logger.info("Categorizing columns by data type")
        
        try:
            categorization = {
                'numerical': list(df.select_dtypes(include=[np.number]).columns),
                'categorical': list(df.select_dtypes(include=['object']).columns),
                'datetime': list(df.select_dtypes(include=['datetime64']).columns),
                'boolean': list(df.select_dtypes(include=['bool']).columns),
                'high_cardinality': [col for col in df.columns if df[col].nunique() > len(df) * 0.8],
                'low_cardinality': [col for col in df.columns if df[col].nunique() < 20 and df[col].nunique() > 1]
            }
            
            logger.info(f"Column categorization completed: "
                       f"{len(categorization['numerical'])} numerical, "
                       f"{len(categorization['categorical'])} categorical, "
                       f"{len(categorization['datetime'])} datetime")
            
            return categorization
            
        except Exception as e:
            logger.error(f"Failed to categorize columns: {str(e)}")
            raise
    
    def _detect_business_entities(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect business-relevant entities"""
        logger.info("Detecting business entities")
        
        try:
            entities = {}
            entities_found = 0
            
            for entity_type, keywords in self.business_keywords.items():
                detected_columns = []
                
                for col in df.columns:
                    col_lower = col.lower().strip()
                    
                    if any(keyword in col_lower for keyword in keywords):
                        confidence = self._calculate_business_confidence(df[col], entity_type)
                        if confidence > 0.3:
                            detected_columns.append({
                                'column': col,
                                'confidence': confidence,
                                'reasoning': f"Column name contains '{entity_type}' keywords"
                            })
                            entities_found += 1
                
                if detected_columns:
                    entities[entity_type] = detected_columns
            
            logger.info(f"Business entity detection completed: found {entities_found} business entities")
            return entities
            
        except Exception as e:
            logger.error(f"Failed to detect business entities: {str(e)}")
            return {}
    
    def _calculate_business_confidence(self, series: pd.Series, entity_type: str) -> float:
        """Calculate confidence that a column represents a business entity"""
        logger.debug(f"Calculating business confidence for entity type: {entity_type}")
        
        try:
            confidence = 0.5
            
            if entity_type == 'revenue' and pd.api.types.is_numeric_dtype(series):
                if (series > 0).sum() / len(series) > 0.8:
                    confidence += 0.3
            
            elif entity_type == 'date':
                if pd.api.types.is_datetime64_any_dtype(series):
                    confidence += 0.4
            
            elif entity_type == 'geography':
                if series.dtype == 'object' and series.nunique() < len(series) * 0.1:
                    confidence += 0.2
            
            return min(confidence, 1.0)
            
        except Exception as e:
            logger.warning(f"Error calculating business confidence: {str(e)}")
            return 0.5
    
    def _assess_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess overall data quality"""
        logger.info("Assessing data quality")
        
        try:
            total_cells = df.shape[0] * df.shape[1]
            null_cells = df.isnull().sum().sum()
            
            quality_assessment = {
                'overall_completeness': round((1 - null_cells / total_cells) * 100, 2),
                'columns_with_nulls': int((df.isnull().sum() > 0).sum()),
                'fully_complete_columns': int((df.isnull().sum() == 0).sum()),
                'duplicate_rows': int(df.duplicated().sum()),
                'constant_columns': [col for col in df.columns if df[col].nunique() <= 1],
                'potential_issues': self._identify_potential_issues(df)
            }
            
            logger.info(f"Data quality assessment completed: {quality_assessment['overall_completeness']}% completeness")
            return quality_assessment
            
        except Exception as e:
            logger.error(f"Failed to assess data quality: {str(e)}")
            return {'overall_completeness': 0, 'potential_issues': ['Error assessing data quality']}
    
    def _detect_relationships(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect potential relationships between columns"""
        logger.info("Detecting column relationships")
        
        try:
            relationships = {
                'potential_hierarchies': [],
                'high_correlations': []
            }
            
            categorical_cols = df.select_dtypes(include=['object']).columns
            
            for i, col1 in enumerate(categorical_cols):
                for col2 in categorical_cols[i+1:]:
                    hierarchy_strength = self._check_hierarchy_relationship(df, col1, col2)
                    if hierarchy_strength > 0.7:
                        relationships['potential_hierarchies'].append({
                            'parent': col1,
                            'child': col2,
                            'strength': hierarchy_strength
                        })
            
            numerical_cols = df.select_dtypes(include=[np.number]).columns
            if len(numerical_cols) > 1:
                corr_matrix = df[numerical_cols].corr()
                for i, col1 in enumerate(numerical_cols):
                    for col2 in numerical_cols[i+1:]:
                        corr_value = corr_matrix.loc[col1, col2]
                        if abs(corr_value) > 0.7:
                            relationships['high_correlations'].append({
                                'column1': col1,
                                'column2': col2,
                                'correlation': round(corr_value, 3)
                            })
            
            logger.info(f"Relationship detection completed")
            return relationships
            
        except Exception as e:
            logger.error(f"Failed to detect relationships: {str(e)}")
            return {'potential_hierarchies': [], 'high_correlations': []}
    
    def _suggest_analyses(self, df: pd.DataFrame) -> List[str]:
        """Suggest potential analyses based on schema"""
        logger.info("Generating analysis suggestions")
        
        try:
            suggestions = []
            
            numerical_cols = df.select_dtypes(include=[np.number]).columns
            categorical_cols = df.select_dtypes(include=['object']).columns
            datetime_cols = df.select_dtypes(include=['datetime64']).columns
            
            if len(numerical_cols) > 0:
                suggestions.append(f"Calculate totals and averages for {', '.join(numerical_cols[:3])}")
            
            if len(categorical_cols) > 0 and len(numerical_cols) > 0:
                suggestions.append(f"Analyze {numerical_cols[0]} by {categorical_cols[0]}")
            
            if len(datetime_cols) > 0 and len(numerical_cols) > 0:
                suggestions.append(f"Analyze trends over time using {datetime_cols[0]}")
            
            business_entities = self._detect_business_entities(df)
            if 'revenue' in business_entities:
                suggestions.append("Revenue analysis: totals, trends, and breakdowns")
            if 'geography' in business_entities:
                suggestions.append("Geographic performance analysis")
            
            final_suggestions = suggestions[:5]
            logger.info(f"Generated {len(final_suggestions)} analysis suggestions")
            return final_suggestions
            
        except Exception as e:
            logger.error(f"Failed to suggest analyses: {str(e)}")
            return ["Basic data exploration and summary statistics"]
    
    def _get_safe_sample_values(self, series: pd.Series, n: int = 3) -> List:
        """Get sample values safely handling different data types"""
        try:
            sample = series.dropna().head(n)
            return [str(val) for val in sample.tolist()]
        except Exception as e:
            logger.warning(f"Error getting sample values: {str(e)}")
            return []
    
    def _analyze_column_characteristics(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze specific characteristics - recognizes normalized types"""
        try:
            characteristics = {}
            
            if pd.api.types.is_datetime64_any_dtype(series):
                characteristics.update({
                    'data_type': 'datetime',
                    'min_date': str(series.min()) if pd.notna(series.min()) else None,
                    'max_date': str(series.max()) if pd.notna(series.max()) else None,
                    'date_format': 'ISO 8601 (normalized)'
                })
            
            elif pd.api.types.is_numeric_dtype(series):
                characteristics.update({
                    'data_type': 'numeric',
                    'min_value': float(series.min()) if pd.notna(series.min()) else None,
                    'max_value': float(series.max()) if pd.notna(series.max()) else None,
                    'mean_value': round(float(series.mean()), 2) if pd.notna(series.mean()) else None,
                    'has_negative_values': bool((series < 0).any()),
                    'all_integers': bool(series.dropna().apply(lambda x: float(x).is_integer()).all()) if len(series.dropna()) > 0 else False
                })
            
            elif series.dtype == 'object':
                characteristics.update({
                    'data_type': 'string',
                    'avg_string_length': round(series.astype(str).str.len().mean(), 1) if len(series.dropna()) > 0 else 0
                })
            
            return characteristics
            
        except Exception as e:
            logger.warning(f"Error analyzing column characteristics: {str(e)}")
            return {}
    
    def _identify_potential_issues(self, df: pd.DataFrame) -> List[str]:
        """Identify potential data quality issues"""
        try:
            issues = []
            
            high_null_cols = [col for col in df.columns if df[col].isnull().sum() / len(df) > 0.5]
            if high_null_cols:
                issues.append(f"High missing values in: {', '.join(high_null_cols[:3])}")
            
            if df.duplicated().sum() > 0:
                issues.append(f"{df.duplicated().sum()} duplicate rows found")
            
            constant_cols = [col for col in df.columns if df[col].nunique() <= 1]
            if constant_cols:
                issues.append(f"Constant columns: {', '.join(constant_cols)}")
            
            return issues
            
        except Exception as e:
            logger.warning(f"Error identifying potential issues: {str(e)}")
            return []
    
    def _check_hierarchy_relationship(self, df: pd.DataFrame, col1: str, col2: str) -> float:
        """Check if two columns have a hierarchical relationship"""
        try:
            grouped = df.groupby(col1)[col2].nunique()
            if grouped.max() == 1:
                return 1.0
            elif grouped.mean() < 2:
                return 0.8
            else:
                return 0.0
        except Exception as e:
            logger.warning(f"Error checking hierarchy: {str(e)}")
            return 0.0
    
    def _get_sample_data(self, df: pd.DataFrame, n: int = 3) -> Dict[str, Any]:
        """Get sample data"""
        logger.info("Extracting sample data")
        
        try:
            sample_data = {
                'first_rows': df.head(n).astype(str).to_dict(),
                'data_preview': {
                    col: [str(val) for val in df[col].dropna().head(n).tolist()]
                    for col in df.columns
                }
            }
            logger.info("Sample data extracted successfully")
            return sample_data
            
        except Exception as e:
            logger.error(f"Error extracting sample data: {str(e)}")
            return {'first_rows': {}, 'data_preview': {}}
    
    def detect_multi_sheet_schema(self, sheets_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Detect schema for all sheets"""
        logger.info(f"Starting multi-sheet schema detection for {len(sheets_dict)} sheets")
        
        try:
            multi_schema = {
                'sheet_count': len(sheets_dict),
                'sheet_names': list(sheets_dict.keys()),
                'sheets': {},
                'column_to_sheet_map': {},
                'all_columns': [],
                'cross_sheet_analysis': {}
            }
            
            for sheet_name, df in sheets_dict.items():
                logger.info(f"Analyzing sheet: {sheet_name}")
                sheet_schema = self.detect_complete_schema(df)
                multi_schema['sheets'][sheet_name] = sheet_schema
                
                for col in df.columns:
                    col_lower = col.lower()
                    if col_lower not in multi_schema['column_to_sheet_map']:
                        multi_schema['column_to_sheet_map'][col_lower] = []
                    multi_schema['column_to_sheet_map'][col_lower].append({
                        'sheet': sheet_name,
                        'original_name': col,
                        'dtype': str(df[col].dtype)
                    })
                    
                    if col not in multi_schema['all_columns']:
                        multi_schema['all_columns'].append(col)
            
            multi_schema['cross_sheet_analysis'] = self._analyze_sheet_relationships(sheets_dict)
            
            logger.info(f"Multi-sheet schema detection completed")
            return multi_schema
            
        except Exception as e:
            logger.error(f"Failed to detect multi-sheet schema: {str(e)}", exc_info=True)
            raise

    def _analyze_sheet_relationships(self, sheets_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Analyze relationships between sheets"""
        logger.info("Analyzing relationships between sheets")
        
        try:
            relationships = {
                'common_columns': [],
                'potential_join_keys': []
            }
            
            sheet_names = list(sheets_dict.keys())
            
            for i, sheet1 in enumerate(sheet_names):
                for sheet2 in sheet_names[i+1:]:
                    common = set(sheets_dict[sheet1].columns) & set(sheets_dict[sheet2].columns)
                    if common:
                        relationships['common_columns'].append({
                            'sheet1': sheet1,
                            'sheet2': sheet2,
                            'columns': list(common)
                        })
                        
                        for col in common:
                            if (sheets_dict[sheet1][col].nunique() == len(sheets_dict[sheet1]) or
                                sheets_dict[sheet2][col].nunique() == len(sheets_dict[sheet2])):
                                relationships['potential_join_keys'].append({
                                    'sheet1': sheet1,
                                    'sheet2': sheet2,
                                    'key_column': col
                                })
            
            logger.info(f"Sheet relationship analysis complete")
            return relationships
            
        except Exception as e:
            logger.error(f"Error analyzing sheet relationships: {str(e)}")
            return {'common_columns': [], 'potential_join_keys': []}