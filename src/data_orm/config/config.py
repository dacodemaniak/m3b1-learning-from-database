import os
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/credit_db")

EXCLUDE_COLUMNS: List[str] = ['orientation_sexuelle']

COLUMN_MAPPING: Dict[str, str] = {
        'nom': 'lastname',
        'prenom': 'firstname',
        'taille': 'height',
        'poids': 'weight',
        'sexe': 'gender',
        'sport_licence': 'sport_licence',
        'smoker': 'smoker',
        'nationalité_francaise': 'french_nationality',
        'revenu_estime_mois': 'estimated_revenue',
        'historique_credits': 'credit_history',
        'risque_personnel': 'personal_risk',
        'date_creation_compte': 'account_creation_date',
        'score_credit': 'credit_score',
        'loyer_mensuel': 'mensual_home_rent',
        'montant_pret': 'credit_amount',
        
        # --- NOUVEAUX MAPPAGES (child_number et caf_quotient) ---
        'nb_enfants': 'child_number',
        'quotient_caf': 'caf_quotient',
        # --------------------------------------------------------
    }