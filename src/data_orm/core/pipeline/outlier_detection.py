from data_orm.config.logger import get_logger
import pandas as pd
import numpy as np
from scipy import stats
from sklearn.ensemble import IsolationForest
from typing import List, Dict

logger = get_logger()

class OutlierDetection:
    """Detects outliers and abnormal distributions"""
    
    def __init__(self):
        self.outlier_report = {}
    
    def detect_outliers(self, df: pd.DataFrame, 
                       methods: List[str] = ['iqr', 'zscore', 'isolation_forest'],
                       contamination: float = 0.1) -> Dict:
        """
        Detect outliers using multiple methods
        
        Args:
            df: Input DataFrame
            methods: List of outlier detection methods
            contamination: Expected proportion of outliers
            
        Returns:
            Dictionary with outlier information
        """
        logger.info("Detecting outliers...")
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        outlier_info = {}
        
        for col in numerical_cols:
            outlier_info[col] = {}
            col_data = df[col].dropna()
            
            if 'iqr' in methods:
                outlier_info[col]['iqr'] = self._detect_iqr_outliers(col_data)
            
            if 'zscore' in methods:
                outlier_info[col]['zscore'] = self._detect_zscore_outliers(col_data)
            
            if 'isolation_forest' in methods and len(numerical_cols) > 1:
                outlier_info[col]['isolation_forest'] = self._detect_isolation_forest_outliers(
                    df[numerical_cols], col, contamination
                )
            
            # Log outlier information for this column
            self._log_column_outliers(col, outlier_info[col])
        
        self.outlier_report['outlier_detection'] = outlier_info
        logger.success("Outlier detection completed")
        return outlier_info
    
    def _log_column_outliers(self, col: str, outlier_methods: Dict):
        """Log outlier information for a specific column"""
        total_outliers = set()
        
        for method, info in outlier_methods.items():
            outliers = info.get('outlier_indices', [])
            total_outliers.update(outliers)
            if outliers:
                logger.warning(f"  {col} - {method}: {len(outliers)} outliers ({info.get('outlier_percentage', 0):.2f}%)")
        
        if total_outliers:
            logger.warning(f"Column '{col}' has {len(total_outliers)} total unique outliers")
    
    def _detect_iqr_outliers(self, data: pd.Series) -> Dict:
        """Detect outliers using IQR method"""
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = data[(data < lower_bound) | (data > upper_bound)]
        
        return {
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'outlier_count': len(outliers),
            'outlier_percentage': (len(outliers) / len(data)) * 100,
            'outlier_indices': outliers.index.tolist()
        }
    
    def _detect_zscore_outliers(self, data: pd.Series, threshold: float = 3) -> Dict:
        """Detect outliers using Z-score method"""
        z_scores = np.abs(stats.zscore(data))
        outliers = data[z_scores > threshold]
        
        return {
            'threshold': threshold,
            'outlier_count': len(outliers),
            'outlier_percentage': (len(outliers) / len(data)) * 100,
            'outlier_indices': outliers.index.tolist()
        }
    
    def _detect_isolation_forest_outliers(self, df: pd.DataFrame, col: str, 
                                        contamination: float) -> Dict:
        """Detect outliers using Isolation Forest"""
        # Use only complete cases for Isolation Forest
        complete_data = df.dropna()
        
        if len(complete_data) > 0:
            iso_forest = IsolationForest(contamination=contamination, random_state=42)
            outliers = iso_forest.fit_predict(complete_data)
            outlier_indices = complete_data.index[outliers == -1].tolist()
            
            return {
                'contamination': contamination,
                'outlier_count': len(outlier_indices),
                'outlier_percentage': (len(outlier_indices) / len(complete_data)) * 100,
                'outlier_indices': outlier_indices
            }
        else:
            return {
                'contamination': contamination,
                'outlier_count': 0,
                'outlier_percentage': 0,
                'outlier_indices': []
            }
    
    def detect_abnormal_distributions(self, df: pd.DataFrame) -> Dict:
        """Detect abnormal distributions using statistical tests"""
        logger.info("Analyzing data distributions...")
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        distribution_info = {}
        
        for col in numerical_cols:
            col_data = df[col].dropna()
            distribution_info[col] = {}
            
            # Normality test
            if len(col_data) > 3:
                stat, p_value = stats.normaltest(col_data)
                distribution_info[col]['normality_test'] = {
                    'statistic': stat,
                    'p_value': p_value,
                    'is_normal': p_value > 0.05
                }
            
            # Skewness and kurtosis
            distribution_info[col]['skewness'] = stats.skew(col_data)
            distribution_info[col]['kurtosis'] = stats.kurtosis(col_data)
            
            # Distribution type assessment
            skewness = abs(distribution_info[col]['skewness'])
            if skewness < 0.5:
                dist_type = 'approximately symmetric'
            elif skewness < 1:
                dist_type = 'moderately skewed'
            else:
                dist_type = 'highly skewed'
            distribution_info[col]['distribution_type'] = dist_type
            
            # Log distribution information
            normality = distribution_info[col].get('normality_test', {}).get('is_normal', False)
            logger.info(f"  {col}: {dist_type} (skewness: {skewness:.3f}, normal: {normality})")
        
        self.outlier_report['distribution_analysis'] = distribution_info
        logger.success("Distribution analysis completed")
        return distribution_info