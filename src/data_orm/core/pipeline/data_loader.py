from typing import Optional
import pandas as pd
from data_orm.core.ports.processor_chain import DataProcessor
from data_orm.infrastructure.file_manager import FileManager
from data_orm.config.logger import get_logger
import httpx


logger = get_logger()

API_BASE_URL = "http://127.0.0.1:8888"
TRAINING_END_POINT="/api/v1/persons/training/datas"
DEFAULT_API_URI=f"{API_BASE_URL}{TRAINING_END_POINT}"

class DataLoader(DataProcessor):
    """Load data from source file"""
    
    def __init__(self, api_url: Optional[str]=DEFAULT_API_URI):
        super().__init__()
        self.file_manager = FileManager()
        self.api_url = api_url
    
    async def _process(self, data: pd.DataFrame, context) -> pd.DataFrame:
        # If data is already loaded (for testing), return it
        if data is not None and not data.empty:
            logger.info("Using pre-loaded data")
            return data
        
        #loaded_data = Optional[pd.DataFrame] = None
        
        if context.input_file:
            # Load data from file
            try:
                logger.info(f"Loading data from: {context.input_file}")
                loaded_data = self.file_manager.load_data(context.input_file)
            except Exception as e:
                logger.error(f"Unable to read {context.input_file} =: {e}")
        if self.api_url:
            try:
                logger.info("Load data from API")
            
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(self.api_url)
                    response.raise_for_status()

                    json_data = response.json()

                    if not json_data:
                        loaded_data = pd.DataFrame()
                        logger.warning("API returns an empty list")
                    else:
                        loaded_data = pd.DataFrame.from_records(json_data)
            except httpx.HTTPStatusError as e:
                logger.error(f"Error while fetching datas HTTP ERROR : {e} => {e.response.text}")
            except httpx.RequestError as e:
                logger.error(f"Error while fetching datas NETWORK ERROR : {e}")
            except Exception as e:
                logger.error(f"Unexpected error during API call : {e}")
                raise RuntimeError(f"Unexpected error during API call : {e}")
        else:
            raise Exception("No suitable data provider was found")
        
        # Store initial metrics
        context.add_statistic('initial_shape', loaded_data.shape)
        context.add_statistic('initial_columns', list(loaded_data.columns))
        
        return loaded_data