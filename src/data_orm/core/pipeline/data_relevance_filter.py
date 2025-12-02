# src/data/infrastructure/pipeline/data_relevance_filter.py

import pandas as pd
from typing import Dict, Any
from ..core.processor_chain import DataProcessor
from data_orm.config.logger import get_logger

logger = get_logger()

class DataRelevanceFilter(DataProcessor):
    """
    Data filter, keeps ONLY relevant columns for the use case.

    - nom (anonymized)
    - prenom (anonymized) 
    - revenu_estime_mois
    - loyer_mensuel
    - montant_pret
    """
    
    def __init__(self, config: Dict = None):
        super().__init__()
        self.config = config or {}
        
        # Keep only these columns
        self.authorized_columns = {
            'nom': {
                'required': True,
                'reason': 'Required but must be anonymized',
                'anonymization': True,
                'final_use': 'Anonymized identifier'
            },
            'prenom': {
                'required': True,
                'reason': 'Required but must be anonymized', 
                'anonymization': True,
                'final_use': 'Anonymized identifier'
            },
            'revenu_estime_mois': {
                'required': True,
                'reason': 'Required, essential for financial analysis',
                'anonymization': False,
                'final_use': 'Financial capacity assessment'
            },
            'loyer_mensuel': {
                'required': True,
                'reason': 'Required, essential for financial analysis',
                'anonymization': False,
                'final_use': 'Housing cost analysis'
            },
            'montant_pret': {
                'required': True,
                'reason': 'Required essential for financial analysis',
                'anonymization': False,
                'final_use': 'Loan amount evaluation'
            }
        }

    def _process(self, data: pd.DataFrame, context: Dict[str, Any]) -> pd.DataFrame:
        logger.info("Starting STRICT data relevance filtering...")
        logger.info("Only 5 columns will be kept: nom, prenom, revenu_estime_mois, loyer_mensuel, montant_pret")
        
        if hasattr(context, 'input_file') and context.input_file:
            input_path = context.input_file
            pre_filter_path = self._generate_pre_filter_path(input_path)
            data.to_csv(pre_filter_path, index=False)
            
            logger.info(f"💾 Saved pre-filter data to: {pre_filter_path}")
            context.pre_filter_file = pre_filter_path
            context.pre_filter_columns = list(data.columns)
            context.pre_filter_shape = data.shape

        # Check for missing required columns
        missing_required = [col for col, info in self.authorized_columns.items() 
                          if info['required'] and col not in data.columns]
        
        if missing_required:
            logger.error(f"CRITICAL: Missing required columns: {missing_required}")
            raise ValueError(f"Required columns missing: {missing_required}")
        
        # Identify columns to remove (everything except the 5 authorized ones)
        columns_to_remove = [col for col in data.columns 
                           if col not in self.authorized_columns.keys()]
        
        # Log the removals
        if columns_to_remove:
            logger.warning(f"Removing {len(columns_to_remove)} columns for compliance")
            for col in columns_to_remove:
                if col in ['age', 'taille', 'poids']:
                    reason = "Discriminatory personal data - removed for ethical compliance"
                elif col in ['historique_credits', 'risque_personnel', 'score_credit']:
                    reason = "Sensitive financial data - removed for data minimization"
                else:
                    reason = "Non-essential data - removed for compliance"
                logger.info(f"Removing column: {col} - {reason}")
        
        # Strict filtering: keep only authorized columns
        filtered_data = data[list(self.authorized_columns.keys())].copy()
        
        # Verify that anonymization has been performed
        anonymization_check = self._verify_anonymization(filtered_data, context)
        
        # Log the final result
        original_cols = len(data.columns)
        final_cols = len(filtered_data.columns)
        
        logger.info(f"STRICT filtering completed:")
        logger.info(f"  - Original columns: {original_cols}")
        logger.info(f"  - Final columns: {final_cols}") 
        logger.info(f"  - Columns removed: {original_cols - final_cols}")
        logger.info(f"  - Columns preserved: {list(filtered_data.columns)}")
        logger.info(f"  - Anonymization status: {anonymization_check}")
        
        # Save the report in the context
        context.relevance_filter_report = {
            'original_columns': list(data.columns),
            'final_columns': list(filtered_data.columns),
            'removed_columns': columns_to_remove,
            'anonymization_verified': anonymization_check
        }
        
        return filtered_data

    def _verify_anonymization(self, data: pd.DataFrame, context: Dict) -> str:
        """
        Verify that anonymization has been performed on nom/prenom
        """
        if 'nom' in data.columns and 'prenom' in data.columns:
            # Basic check - if values are too unique, may not be anonymized
            nom_unique_ratio = data['nom'].nunique() / len(data)
            prenom_unique_ratio = data['prenom'].nunique() / len(data)
            
            if nom_unique_ratio > 0.9 or prenom_unique_ratio > 0.9:
                return "ANONYMIZATION_SUSPECT - high uniqueness ratio"
            else:
                return "ANONYMIZATION_LIKELY_OK"
        return "ANONYMIZATION_NOT_APPLICABLE"
    
    def _generate_pre_filter_path(self, input_path: str) -> str:
        from pathlib import Path
        input_path = Path(input_path)
        pre_filter_name = f"{input_path.stem}_pre_filter{input_path.suffix}"
        return str(input_path.parent / pre_filter_name)