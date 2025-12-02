from typing import Dict, Optional
import pandas as pd

from data_orm.config.logger import get_logger
from data_orm.core.pipeline.pipeline_context import PipelineContext
from data_orm.core.ports.processor_chain import DataProcessor
from data_orm.core.pipeline.data_loader import DataLoader
from data_orm.core.pipeline.data_anonymizer import DataAnonymizer
from data_orm.core.pipeline.statistical_analyzer import StatisticalAnalyzer
from data_orm.core.pipeline.outlier_detector import OutlierDetector
from data_orm.core.pipeline.data_cleaner_processor import DataCleanerProcessor
from data_orm.core.pipeline.data_relevance_filter import DataRelevanceFilter
from data_orm.core.pipeline.data_normalizer import DataNormalizer
from data_orm.core.pipeline.final_analyzer import FinalAnalyzer
from data_orm.core.pipeline.data_saver import DataSaver

logger = get_logger()

class PipelineOrchestrator:
    """Orchestrates the data preprocessing pipeline using Chain of Responsibility"""
    
    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}

        self.processor_chain = self._build_processor_chain()

    
    def _build_processor_chain(self) -> DataProcessor:
        """Build the chain of data processors"""
        
        # Create processors with configuration
        data_loader = DataLoader()

        data_anonymizer = DataAnonymizer(
            strategy=self.config.get('anonymization', {}).get('strategy', 'hash')
        )

        statistical_analyzer = StatisticalAnalyzer()

        outlier_detector = OutlierDetector(
            methods=self.config.get('outlier_detection', {}).get('methods', ['iqr', 'zscore'])
        )

        data_cleaner = DataCleanerProcessor(
            missing_strategy=self.config.get('cleaning', {}).get('missing_values_strategy', 'auto'),
            outlier_method=self.config.get('cleaning', {}).get('outlier_removal_method', 'iqr')
        )

        data_relevance_filter = DataRelevanceFilter(
            config=self.config.get('relevance_filter', {})
        )

        data_normalizer = DataNormalizer(
            normalization_method=self.config.get('normalization', {}).get('method', 'minmax')
        )
        final_analyzer = FinalAnalyzer()

        data_saver = DataSaver()
        
        # Build the chain
        data_loader.set_next(data_anonymizer) \
                  .set_next(statistical_analyzer) \
                  .set_next(outlier_detector) \
                  .set_next(data_cleaner) \
                  .set_next(data_relevance_filter) \
                  .set_next(data_normalizer) \
                  .set_next(final_analyzer) \
                  .set_next(data_saver)
        
        return data_loader
    
    def run_pipeline(self, input_file: Optional[str] = None, output_file: Optional[str] = None) -> pd.DataFrame:
        """
        Run the complete data preprocessing pipeline
        
        Args:
            input_file: Path to input CSV file
            output_file: Path for output CSV file (optional)
            
        Returns:
            Processed DataFrame
        """
        logger.info("🚀 Starting Chain of Responsibility Data Pipeline")
        
        try:
            # Create pipeline context
            context = PipelineContext(
                config=self.config
            )
            
            # Start the processing chain with empty data (will be loaded by DataLoader)
            initial_data = pd.DataFrame()
            processed_data = self.processor_chain.process(initial_data, context)
            
            logger.success("🎉 Pipeline completed successfully!")
            return processed_data
            
        except Exception as e:
            logger.error(f"💥 Pipeline failed: {str(e)}")
            raise
    
    def run_pipeline_with_data(self, data: pd.DataFrame, output_file: Optional[str] = None) -> pd.DataFrame:
        """
        Run pipeline with pre-loaded data (useful for testing)
        
        Args:
            data: Pre-loaded DataFrame
            output_file: Path for output CSV file (optional)
            
        Returns:
            Processed DataFrame
        """
        logger.info("🚀 Starting pipeline with pre-loaded data")
        
        context = PipelineContext(
            input_file="pre_loaded_data",
            output_file=output_file,
            config=self.config
        )
        
        processed_data = self.processor_chain.process(data, context)

        self._generate_compliance_report(context)

        return processed_data
    
    def _generate_compliance_report(self, context: PipelineContext):
        report = context.get('relevance_filter_report')
        if report is not None:
            logger.info("🔒 COMPLIANCE REPORT:")
            logger.info(f"   Original columns: {len(report['original_columns'])}")
            logger.info(f"   Final columns: {len(report['final_columns'])}")
            logger.info(f"   Data minimization: {len(report['removed_columns'])} columns removed")
            logger.info(f"   Preserved columns: {report['final_columns']}")
            logger.info(f"   Anonymization: {report['anonymization_verified']}")