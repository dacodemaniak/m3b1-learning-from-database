from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
from data_orm.config.logger import get_logger
from data_orm.core.pipeline.pipeline_context import PipelineContext

logger = get_logger()

class DataProcessor(ABC):
    """Abstract base class for all data processors in the chain"""
    
    def __init__(self):
        self._next_processor = None
    
    def set_next(self, processor: 'DataProcessor') -> 'DataProcessor':
        """Set the next processor in the chain"""
        self._next_processor = processor
        return processor
    
    def process(self, data: pd.DataFrame, context: PipelineContext) -> pd.DataFrame:
        """
        Process data and pass to next processor in chain
        
        Args:
            data: Input DataFrame
            context: Shared context dictionary for passing information between processors
            
        Returns:
            Processed DataFrame
        """
        try:
            logger.info(f"🔧 Processing: {self.__class__.__name__}")
            processed_data = self._process(data, context)
            
            if self._next_processor:
                return self._next_processor.process(processed_data, context)
            
            return processed_data
            
        except Exception as e:
            logger.error(f"❌ Error in {self.__class__.__name__}: {str(e)}")
            raise
    
    @abstractmethod
    def _process(self, data: pd.DataFrame, context: PipelineContext) -> pd.DataFrame:
        """Abstract method to be implemented by concrete processors"""
        pass