import pandas as pd
from ..core.processor_chain import DataProcessor
from .data_cleaner import DataCleaner
from data_orm.config.logger import get_logger

logger = get_logger()

class DataCleanerProcessor(DataProcessor):
    """Clean data by handling missing values and outliers"""
    
    def __init__(self, missing_strategy: str = 'auto', outlier_method: str = 'iqr'):
        super().__init__()
        self.missing_strategy = missing_strategy
        self.outlier_method = outlier_method
        self.data_cleaner = DataCleaner()
    
    def _process(self, data: pd.DataFrame, context) -> pd.DataFrame:
        logger.info("Cleaning data")
        
        # Get outlier report from context
        outlier_report = context.results.get('outlier_detection', {}).get('outlier_report', {})
        
        # Handle missing values
        data = self.data_cleaner.handle_missing_values(
            data, 
            strategy=self.missing_strategy,
            numerical_strategy='mean',
            categorical_strategy='most_frequent'
        )
        
        # Remove outliers
        data = self.data_cleaner.remove_outliers(data, outlier_report, method=self.outlier_method)
        
        # Store cleaning results
        context.add_result('data_cleaning', {
            'cleaning_report': self.data_cleaner.cleaning_report,
            'final_shape': data.shape
        })
        
        context.add_statistic('final_shape_after_cleaning', data.shape)
        
        return data