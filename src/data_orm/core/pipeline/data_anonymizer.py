import pandas as pd
from ..core.processor_chain import DataProcessor
from .anonymizer import Anonymizer
from data_orm.config.logger import get_logger

logger = get_logger()

class DataAnonymizer(DataProcessor):
    """Anonymize sensitive data"""
    
    def __init__(self, strategy: str = 'hash'):
        super().__init__()
        self.strategy = strategy
        self.anonymizer = Anonymizer()
    
    def _process(self, data: pd.DataFrame, context) -> pd.DataFrame:
        logger.info("Starting data anonymization")
        
        # Detect sensitive columns
        explicit_sensitive = context.config.get('anonymization', {}).get('explicit_sensitive_columns', [])
        sensitive_columns = self.anonymizer.detect_sensitive_columns(data, explicit_sensitive)
        
        if sensitive_columns:
            logger.info(f"Anonymizing {len(sensitive_columns)} sensitive columns")
            data = self.anonymizer.anonymize_data(data, strategy=self.strategy)
            context.add_result('anonymization', {
                'sensitive_columns': sensitive_columns,
                'strategy': self.strategy,
                'anonymization_report': self.anonymizer.anonymization_report
            })
        else:
            logger.info("No sensitive columns detected")
        
        return data