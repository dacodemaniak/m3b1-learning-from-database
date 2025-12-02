import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Any
from sqlalchemy.orm import Session
from data_orm.domain.person import Person
from data_orm.domain.area import Area
from data_orm.domain.family_situation import FamilySituation
from data_orm.domain.study_level import StudyLevel
# Import des utilitaires
from data_orm.config.parse_and_convert_utils import convert_bool, parse_date

class CSVDataLoader:
    def __init__(self, session: Session):
        self.session = session
        self.study_levels: Dict[str, int] = {}
        self.areas: Dict[str, int] = {}
        self.family_situations: Dict[str, int] = {}
        
    def _load_reference_data(self, df: pd.DataFrame, column_name: str, ORM_Class, cache: Dict[str, int]) -> Dict[str, int]:
        """Méthode générique pour charger les données de référence (étude, région, situation familiale)"""
        # S'assurer que le nom de colonne est une chaîne pour .unique()
        unique_labels: Any = df[column_name].dropna().astype(str).unique()
        
        for label in unique_labels:
            if label not in cache:
                # Vérifie si l'enregistrement existe déjà
                existing = self.session.query(ORM_Class).filter(ORM_Class.label == label).first()
                if existing:
                    cache[label] = existing.id
                else:
                    # Crée un nouvel enregistrement
                    new_item = ORM_Class(label=label)
                    self.session.add(new_item)
                    self.session.flush()  # Pour obtenir l'ID
                    cache[label] = new_item.id
        
        return cache
        
    def load_study_levels(self, csv_path: str) -> Dict[str, int]:
        """Charge et insère les niveaux d'étude uniques"""
        df = pd.read_csv(csv_path)
        # Utilisation de la méthode générique
        self.study_levels = self._load_reference_data(df, 'niveau_etude', StudyLevel, self.study_levels)
        self.session.commit()
        return self.study_levels
    
    def load_areas(self, csv_path: str) -> Dict[str, int]:
        """Charge et insère les régions uniques"""
        df = pd.read_csv(csv_path)
        self.areas = self._load_reference_data(df, 'region', Area, self.areas)
        self.session.commit()
        return self.areas
    
    def load_family_situations(self, csv_path: str) -> Dict[str, int]:
        """Charge et insère les situations familiales uniques"""
        df = pd.read_csv(csv_path)
        self.family_situations = self._load_reference_data(df, 'situation_familiale', FamilySituation, self.family_situations)
        self.session.commit()
        return self.family_situations
    
    # Suppression des méthodes convert_bool et parse_date
    
    def load_persons(self, csv_path: str):
        """Charge et insère les personnes"""
        # 1. Charge d'abord les références (optimisé avec la méthode générique)
        self.load_study_levels(csv_path)
        self.load_areas(csv_path)
        self.load_family_situations(csv_path)
        
        df = pd.read_csv(csv_path)
        
        # 2. Itération et insertion des personnes
        for count, (_, row) in enumerate(df.iterrows()):
            
            # Gestion des colonnes de référence qui peuvent être NaN (None en Python)
            # IMPORTANT: La conversion en str() doit être faite sur la valeur non-NaN.
            study_level_label = str(row['niveau_etude']) if pd.notna(row['niveau_etude']) else None
            area_label = str(row['region']) if pd.notna(row['region']) else None
            family_situation_label = str(row['situation_familiale']) if pd.notna(row['situation_familiale']) else None
            
            # Récupération des IDs (utilisation de .get() avec vérification de la clé)
            study_level_id = self.study_levels.get(study_level_label) if study_level_label else None
            area_id = self.areas.get(area_label) if area_label else None
            family_situation_id = self.family_situations.get(family_situation_label) if family_situation_label else None
            
            person = Person(
                lastname=str(row['nom']),
                firstname=str(row['prenom']),
                # Utilisation de .item() pour extraire la valeur nativement, ou vérification des types
                age=int(row['age']) if pd.notna(row['age']) else None, # Assurez-vous que le modèle accepte None
                height=float(row['taille']) if pd.notna(row['taille']) else None,
                weight=float(row['poids']) if pd.notna(row['poids']) else None,
                gender=str(row['sexe']),
                # Utilisation des fonctions utilitaires importées
                sport_licence=convert_bool(row['sport_licence']),
                smoker=convert_bool(row['smoker']),
                french_nationality=convert_bool(row['nationalité_francaise']),
                estimated_revenue=float(row['revenu_estime_mois']) if pd.notna(row['revenu_estime_mois']) else None,
                credit_history=float(row['historique_credits']) if pd.notna(row['historique_credits']) else None,
                personal_risk=float(row['risque_personnel']) if pd.notna(row['risque_personnel']) else None,
                account_creation_date=parse_date(row['date_creation_compte']),
                credit_score=float(row['score_credit']) if pd.notna(row['score_credit']) else None,
                mensual_home_rent=float(row['loyer_mensuel']) if pd.notna(row['loyer_mensuel']) else None,
                credit_amount=float(row['montant_pret']) if pd.notna(row['montant_pret']) else None,
                
                # Assignation des IDs récupérés
                study_level_id=study_level_id,
                area_id=area_id,
                family_situation_id=family_situation_id
            )
            
            self.session.add(person)
            
            # Commit par lots de 100 pour la performance
            if count % 100 == 0:
                self.session.commit()
        
        # Commit final
        self.session.commit()