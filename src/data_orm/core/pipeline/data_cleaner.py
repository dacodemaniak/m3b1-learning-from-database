from data_orm.config.logger import get_logger
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer, KNNImputer
from typing import Dict

logger = get_logger()

class DataCleaner:
    """Handles data cleaning operations"""
    
    def __init__(self):
        self.cleaning_report = {}
    
    def handle_missing_values(self, df: pd.DataFrame, 
                            strategy: str = 'auto',
                            numerical_strategy: str = 'mean',
                            categorical_strategy: str = 'most_frequent',
                            threshold: float = 0.5) -> pd.DataFrame:
        """
        Handle missing values based on specified strategy
        
        Args:
            df: Input DataFrame
            strategy: Overall strategy ('auto', 'delete', 'impute')
            numerical_strategy: Strategy for numerical columns
            categorical_strategy: Strategy for categorical columns
            threshold: Threshold for column deletion (if missing > threshold%)
            
        Returns:
            Cleaned DataFrame
        """
        logger.info(f"Handling missing values with strategy: {strategy}")
        df_clean = df.copy()
        cleaning_details = {}
        
        if strategy == 'auto':
            # Automatic strategy: delete columns with too many missing values, impute others
            missing_percent = (df_clean.isnull().sum() / len(df_clean)) * 100
            
            # Delete columns with missing values above threshold
            cols_to_delete = missing_percent[missing_percent > threshold * 100].index
            if len(cols_to_delete) > 0:
                df_clean.drop(columns=cols_to_delete, inplace=True)
                cleaning_details['deleted_columns'] = cols_to_delete.tolist()
                logger.warning(f"Deleted columns with >{threshold*100}% missing values: {list(cols_to_delete)}")
            
            # Impute remaining missing values
            df_clean = self._impute_missing_values(df_clean, numerical_strategy, categorical_strategy)
            cleaning_details['imputation_strategy'] = {
                'numerical': numerical_strategy,
                'categorical': categorical_strategy
            }
        
        elif strategy == 'delete':
            # Delete rows with any missing values
            initial_shape = df_clean.shape
            df_clean.dropna(inplace=True)
            cleaning_details['rows_deleted'] = initial_shape[0] - df_clean.shape[0]
            logger.warning(f"Deleted {cleaning_details['rows_deleted']} rows with missing values")
        
        elif strategy == 'impute':
            # Impute all missing values
            df_clean = self._impute_missing_values(df_clean, numerical_strategy, categorical_strategy)
            cleaning_details['imputation_strategy'] = {
                'numerical': numerical_strategy,
                'categorical': categorical_strategy
            }
        
        self.cleaning_report['missing_values_handling'] = cleaning_details
        logger.success("Missing values handling completed")
        return df_clean
    
    def _impute_missing_values(self, df: pd.DataFrame, 
                             numerical_strategy: str, 
                             categorical_strategy: str) -> pd.DataFrame:
        """Impute missing values using specified strategies"""
        df_imputed = df.copy()
        
        # Handle numerical columns
        numerical_cols = df_imputed.select_dtypes(include=[np.number]).columns
        if len(numerical_cols) > 0 and df_imputed[numerical_cols].isnull().sum().sum() > 0:
            if numerical_strategy == 'mean':
                imputer = SimpleImputer(strategy='mean')
            elif numerical_strategy == 'median':
                imputer = SimpleImputer(strategy='median')
            elif numerical_strategy == 'knn':
                imputer = KNNImputer(n_neighbors=5)
            else:
                imputer = SimpleImputer(strategy='constant', fill_value=0)
            
            df_imputed[numerical_cols] = imputer.fit_transform(df_imputed[numerical_cols])
            logger.info(f"Imputed numerical columns using {numerical_strategy} strategy")
        
        # Handle categorical columns
        categorical_cols = df_imputed.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0 and df_imputed[categorical_cols].isnull().sum().sum() > 0:
            if categorical_strategy == 'most_frequent':
                imputer = SimpleImputer(strategy='most_frequent')
            else:
                imputer = SimpleImputer(strategy='constant', fill_value='Unknown')
            
            df_imputed[categorical_cols] = imputer.fit_transform(df_imputed[categorical_cols])
            logger.info(f"Imputed categorical columns using {categorical_strategy} strategy")
        
        return df_imputed
    
    def remove_outliers(self, df: pd.DataFrame, outlier_report: Dict, 
                       method: str = 'iqr') -> pd.DataFrame:
        """
        Remove outliers based on detection report
        
        Args:
            df: Input DataFrame
            outlier_report: Report from OutlierDetection
            method: Outlier detection method to use for removal
            
        Returns:
            DataFrame with outliers removed
        """
        logger.info(f"Removing outliers using {method} method")
        df_clean = df.copy()
        removal_details = {}
        
        outlier_info = outlier_report.get('outlier_detection', {})
        all_outlier_indices = set()
        
        for col in outlier_info:
            if method in outlier_info[col]:
                outlier_indices = outlier_info[col][method].get('outlier_indices', [])
                all_outlier_indices.update(outlier_indices)
                if outlier_indices:
                    removal_details[col] = len(outlier_indices)
        
        if all_outlier_indices:
            initial_count = len(df_clean)
            df_clean = df_clean.drop(list(all_outlier_indices))
            removed_count = initial_count - len(df_clean)
            
            logger.warning(f"Removed {removed_count} outliers total")
            for col, count in removal_details.items():
                logger.warning(f"  {col}: {count} outliers removed")
        else:
            logger.info("No outliers to remove")
        
        self.cleaning_report['outlier_removal'] = removal_details
        logger.success("Outlier removal completed")
        return df_clean