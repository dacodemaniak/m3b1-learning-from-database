from typing import Optional
import pandas as pd
from data_orm.core.ports.processor_chain import DataProcessor
from .outlier_detection import OutlierDetection
from data_orm.config.logger import get_logger

logger = get_logger()

class OutlierDetector(DataProcessor):
    """Detect outliers and abnormal distributions"""
    
    def __init__(self, methods: Optional[list] = None):
        super().__init__()
        self.methods = methods or ['iqr', 'zscore', 'isolation_forest']
        self.outlier_detector = OutlierDetection()
    
    def _process(self, data: pd.DataFrame, context) -> pd.DataFrame:
        logger.info("Detecting outliers and abnormal distributions")
        
        # Detect outliers
        outlier_info = self.outlier_detector.detect_outliers(data, methods=self.methods)
        
        # Detect abnormal distributions
        distribution_info = self.outlier_detector.detect_abnormal_distributions(data)
        
        # Store results in context
        context.add_result('outlier_detection', {
            'outlier_info': outlier_info,
            'distribution_info': distribution_info,
            'outlier_report': self.outlier_detector.outlier_report
        })
        
        # Calculate total outliers
        total_outliers = self._calculate_total_outliers(outlier_info)
        context.add_statistic('total_outliers', total_outliers)
        
        return data
    
    def _calculate_total_outliers(self, outlier_info: dict) -> int:
        """Calculate total number of unique outliers across all methods and columns"""
        all_outliers = set()
        for col_info in outlier_info.values():
            for method_info in col_info.values():
                outliers = method_info.get('outlier_indices', [])
                all_outliers.update(outliers)
        return len(all_outliers)