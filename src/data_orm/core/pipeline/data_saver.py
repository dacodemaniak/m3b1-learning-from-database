import pandas as pd
from ..core.processor_chain import DataProcessor
from ..core.file_manager import FileManager
from data_orm.config.logger import get_logger
import json

logger = get_logger()

class DataSaver(DataProcessor):
    """Save processed data and generate reports"""
    
    def __init__(self):
        super().__init__()
        self.file_manager = FileManager()
    
    def _process(self, data: pd.DataFrame, context) -> pd.DataFrame:
        logger.info("Saving processed data and generating reports")
        
        # Save processed data
        if context.output_file:
            self.file_manager.working_data = data
            self.file_manager.save_processed_data(context.output_file)
            logger.success(f"Processed data saved to: {context.output_file}")
        
        # Generate pipeline report
        self._generate_pipeline_report(context)
        
        # Log final metrics
        self._log_final_metrics(context)
        
        return data
    
    def _generate_pipeline_report(self, context):
        """Generate comprehensive pipeline report"""
        pipeline_report = {
            'pipeline_configuration': context.config,
            'processing_results': context.results,
            'statistics': context.statistics,
            'quality_metrics': context.results.get('final_analysis', {}).get('quality_metrics', {}),
            'errors': context.errors
        }
        
        try:
            with open('pipeline_report.json', 'w') as f:
                json.dump(pipeline_report, f, indent=2, default=str)
            logger.info("Pipeline report saved to pipeline_report.json")
        except Exception as e:
            logger.error(f"Could not save pipeline report: {str(e)}")
            context.add_error(f"Report generation failed: {str(e)}")
    
    def _log_final_metrics(self, context):
        """Log final quality metrics"""
        quality_metrics = context.results.get('final_analysis', {}).get('quality_metrics', {})
        
        logger.info("📊 FINAL PIPELINE METRICS:")
        logger.info(f"   • Completeness: {quality_metrics.get('completeness', 0):.4f}")
        logger.info(f"   • Quality Score: {quality_metrics.get('quality_score', 0):.4f}")
        logger.info(f"   • Final Precision: {quality_metrics.get('final_precision', 0):.4f}")
        
        if context.errors:
            logger.warning(f"   • Errors encountered: {len(context.errors)}")
            for error in context.errors:
                logger.warning(f"     - {error}")