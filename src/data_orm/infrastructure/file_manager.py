from typing import Optional
from data_orm.config.logger import get_logger
import pandas as pd
import shutil
from pathlib import Path

logger = get_logger()

class FileManager:
    """Manages file operations and data loading"""
    
    def __init__(self):
        self.original_data = None
        self.working_data = None
        
    def load_data(self, file_path: Optional[str] = None, backup_suffix: str = "_backup") -> pd.DataFrame:
        """
        Load data from CSV file and create backup
        
        Args:
            file_path: Path to the CSV file
            backup_suffix: Suffix for backup file
            
        Returns:
            Loaded DataFrame
        """
        try:
            # Load original data
            if file_path:
                self.original_data = pd.read_csv(file_path)
                logger.info(f"Successfully loaded data from {file_path}")
                logger.info(f"Data shape: {self.original_data.shape}")
                logger.debug(f"Columns: {list(self.original_data.columns)}")
                logger.debug(f"Data types:\n{self.original_data.dtypes}")
                
                # Create backup
                backup_path = self._create_backup(file_path, backup_suffix)
                logger.info(f"Backup created at: {backup_path}")
                
                # Create working copy
                self.working_data = self.original_data.copy()
                
                return self.working_data
            else:
                raise Exception("No file to import, maybe data come from database")
        except Exception as e:
            logger.error(f"Error loading data from {file_path}: {str(e)}")
            raise
    
    def _create_backup(self, file_path: str, suffix: str) -> str:
        """Create backup of original file"""
        path = Path(file_path)
        backup_path = path.parent / f"{path.stem}{suffix}{path.suffix}"
        shutil.copy2(file_path, backup_path)
        return str(backup_path)
    
    def save_processed_data(self, file_path: str):
        """Save processed data to CSV"""
        try:
            if self.working_data is not None:
                self.working_data.to_csv(file_path, index=False)
                logger.success(f"Processed data saved to {file_path}")
                logger.debug(f"Final data shape: {self.working_data.shape}")
            else:
                logger.warning("No working data to save")
        except Exception as e:
            logger.error(f"Error saving data to {file_path}: {str(e)}")
            raise