from data_orm.config.logger import get_logger
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from typing import Dict

logger = get_logger()

class NormalizerStandardizer:
    """Handles data normalization and standardization"""
    
    def __init__(self):
        self.transformation_report = {}
    
    def normalize_data(self, df: pd.DataFrame, 
                      method: str = 'minmax',
                      range: tuple = (0, 1)) -> pd.DataFrame:
        """
        Normalize numerical data
        
        Args:
            df: Input DataFrame
            method: Normalization method ('minmax', 'maxabs')
            range: Target range for minmax normalization
            
        Returns:
            Normalized DataFrame
        """
        logger.info(f"Normalizing data using {method} method")
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        df_normalized = df.copy()
        
        if method == 'minmax':
            scaler = MinMaxScaler(feature_range=range)
        elif method == 'maxabs':
            from sklearn.preprocessing import MaxAbsScaler
            scaler = MaxAbsScaler()
        else:
            raise ValueError(f"Unknown normalization method: {method}")
        
        if len(numerical_cols) > 0:
            df_normalized[numerical_cols] = scaler.fit_transform(df_normalized[numerical_cols])
            logger.info(f"Normalized {len(numerical_cols)} numerical columns")
            
            self.transformation_report['normalization'] = {
                'method': method,
                'range': range if method == 'minmax' else None,
                'columns': list(numerical_cols)
            }
        
        logger.success("Data normalization completed")
        return df_normalized
    
    def standardize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize numerical data (mean=0, std=1)
        
        Args:
            df: Input DataFrame
            
        Returns:
            Standardized DataFrame
        """
        logger.info("Standardizing data (mean=0, std=1)")
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        df_standardized = df.copy()
        
        if len(numerical_cols) > 0:
            scaler = StandardScaler()
            df_standardized[numerical_cols] = scaler.fit_transform(df_standardized[numerical_cols])
            logger.info(f"Standardized {len(numerical_cols)} numerical columns")
            
            self.transformation_report['standardization'] = {
                'method': 'standard_scaler',
                'columns': list(numerical_cols),
                'mean': scaler.mean_.tolist(),
                'scale': scaler.scale_.tolist()
            }
        
        logger.success("Data standardization completed")
        return df_standardized