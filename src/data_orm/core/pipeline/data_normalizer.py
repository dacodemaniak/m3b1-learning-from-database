import pandas as pd
from ..core.processor_chain import DataProcessor
from .normalizer_standardizer import NormalizerStandardizer
from data_orm.config.logger import get_logger

logger = get_logger()

class DataNormalizer(DataProcessor):
    """Normalize and standardize data"""
    
    def __init__(self, normalization_method: str = 'minmax'):
        super().__init__()
        self.normalization_method = normalization_method
        self.normalizer = NormalizerStandardizer()
    
    def _process(self, data: pd.DataFrame, context) -> pd.DataFrame:
        logger.info("Normalizing and standardizing data")
        
        # Normalize data
        data = self.normalizer.normalize_data(data, method=self.normalization_method)
        
        # Standardize data
        data = self.normalizer.standardize_data(data)
        
        # Store transformation results
        context.add_result('data_transformation', {
            'transformation_report': self.normalizer.transformation_report,
            'final_data_info': {
                'shape': data.shape,
                'columns': list(data.columns),
                'data_types': data.dtypes.to_dict()
            }
        })
        
        return data