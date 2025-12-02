import pandas as pd
from data_orm.core.ports.processor_chain import DataProcessor
from .statistizer import Statistizer
from data_orm.config.logger import get_logger

logger = get_logger()

class StatisticalAnalyzer(DataProcessor):
    """Perform statistical analysis"""
    
    def __init__(self):
        super().__init__()
        self.statistizer = Statistizer()
    
    def _process(self, data: pd.DataFrame, context) -> pd.DataFrame:
        logger.info("Performing statistical analysis")
        
        # Generate descriptive statistics
        stats = self.statistizer.generate_descriptive_stats(data)
        missing_analysis = self.statistizer.analyze_missing_values(data)
        
        # Create visualizations
        self.statistizer.create_visualizations(data)
        
        # Store results in context
        context.add_result('statistical_analysis', {
            'descriptive_stats': stats,
            'missing_analysis': missing_analysis,
            'stats_report': self.statistizer.stats_report
        })
        
        context.add_statistic('missing_percentage', missing_analysis.get('overall_missing_percentage', 0))
        
        return data