from data_orm.config.logger import get_logger
import pandas as pd
import numpy as np
from typing import List, Optional

logger = get_logger()

class Anonymizer:
    """Handles sensitive data detection and anonymization"""
    
    def __init__(self):
        self.sensitive_columns = []
        self.anonymization_report = {}
        
        # Common patterns for sensitive data
        self.sensitive_patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
            'telephone': r'(\+?\d{1,3}[-.\s]?)?\(?\d{2,4}\)?[-.\s]?\d{2,4}[-.\s]?\d{4,6}',
            'ssn': r'\d{3}-\d{2}-\d{4}',
            'credit_card': r'\d{4}-\d{4}-\d{4}-\d{4}',
            'carte': r'\d{4}-\d{4}-\d{4}-\d{4}',
            'cb': r'\d{4}-\d{4}-\d{4}-\d{4}',
            'ccv': r'\b\d{3,4}\b',
            'password': r'.{8,}'  # At least 8 characters
        }
    
    def detect_sensitive_columns(self, df: pd.DataFrame, 
                               explicit_sensitive: Optional[List[str]] = None) -> List[str]:
        """
        Detect columns that may contain sensitive information
        
        Args:
            df: Input DataFrame
            explicit_sensitive: List of explicitly sensitive column names
            
        Returns:
            List of sensitive column names
        """
        sensitive_columns = []
        
        if explicit_sensitive:
            found_columns = [col for col in explicit_sensitive if col in df.columns]
            sensitive_columns.extend(found_columns)
            if found_columns:
                logger.info(f"Explicitly specified sensitive columns: {found_columns}")
        
        # Detect by column names
        sensitive_keywords = ['name', 'email', 'phone', 'address', 'ssn', 'id', 
                             'password', 'credit', 'card', 'birth', 'social', 'security',
                             'nom', 'prenom', 'telephone', 'adresse', 'motdepasse', 'carte', 'cb', 'ccv']
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Check for sensitive keywords in column names
            if any(keyword in col_lower for keyword in sensitive_keywords):
                if col not in sensitive_columns:
                    sensitive_columns.append(col)
                    logger.info(f"Detected sensitive column by name pattern: {col}")
            
            # Check data patterns for string columns
            if df[col].dtype == 'object':
                sample_data = df[col].dropna().head(100)
                if len(sample_data) > 0:
                    for pattern_name, pattern in self.sensitive_patterns.items():
                        if sample_data.astype(str).str.match(pattern).any():
                            if col not in sensitive_columns:
                                sensitive_columns.append(col)
                                logger.info(f"Detected sensitive column by data pattern ({pattern_name}): {col}")
                                break
        
        self.sensitive_columns = sensitive_columns
        self.anonymization_report['sensitive_columns_detected'] = sensitive_columns
        self.anonymization_report['detection_method'] = 'automatic'
        
        if sensitive_columns:
            logger.info(f"Total sensitive columns detected: {len(sensitive_columns)}")
        else:
            logger.info("No sensitive columns detected")
        
        return sensitive_columns
    
    def anonymize_data(self, df: pd.DataFrame, 
                      strategy: str = 'hash',
                      columns_to_anonymize: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Anonymize sensitive columns
        
        Args:
            df: Input DataFrame
            strategy: Anonymization strategy ('hash', 'delete', 'mask')
            columns_to_anonymize: Specific columns to anonymize
            
        Returns:
            Anonymized DataFrame
        """
        if columns_to_anonymize is None:
            columns_to_anonymize = self.sensitive_columns
        
        df_anonymized = df.copy()
        anonymization_details = {}
        
        for col in columns_to_anonymize:
            if col in df_anonymized.columns:
                original_dtype = df_anonymized[col].dtype
                original_sample = str(df_anonymized[col].iloc[0]) if len(df_anonymized) > 0 else "N/A"
                
                if strategy == 'delete':
                    df_anonymized.drop(columns=[col], inplace=True)
                    action = 'deleted'
                    
                elif strategy == 'hash':
                    # Create deterministic hash for consistency
                    df_anonymized[col] = df_anonymized[col].astype(str).apply(
                        lambda x: str(abs(hash(x))) if pd.notna(x) and x != 'nan' else np.nan
                    )
                    action = 'hashed'
                    
                elif strategy == 'mask':
                    if df_anonymized[col].dtype == 'object':
                        df_anonymized[col] = df_anonymized[col].apply(
                            lambda x: '***MASKED***' if pd.notna(x) and x != 'nan' else np.nan
                        )
                    else:
                        df_anonymized[col] = 0
                    action = 'masked'
                
                anonymized_sample = str(df_anonymized[col].iloc[0]) if col in df_anonymized.columns and len(df_anonymized) > 0 else "N/A"
                
                anonymization_details[col] = {
                    'action': action,
                    'original_dtype': str(original_dtype),
                    'strategy': strategy,
                    'original_sample': original_sample,
                    'anonymized_sample': anonymized_sample
                }
                
                logger.info(f"Anonymized column '{col}': {action}")
                logger.debug(f"  Original sample: {original_sample}")
                logger.debug(f"  Anonymized sample: {anonymized_sample}")
        
        self.anonymization_report['anonymization_details'] = anonymization_details
        logger.success(f"Anonymization completed using '{strategy}' strategy")
        
        return df_anonymized