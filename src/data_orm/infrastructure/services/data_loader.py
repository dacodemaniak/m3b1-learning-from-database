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
from data_orm.config.config import COLUMN_MAPPING, EXCLUDE_COLUMNS
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
        count = 0

        df = df.drop(columns=EXCLUDE_COLUMNS, errors='ignore')

        # 2. Itération et insertion des personnes
        for _, row in df.iterrows():
            
            count += 1
            
            # Récupération des IDs (utilisation de .get() avec vérification de la clé)
            study_level_id = self.study_levels.get(row['niveau_etude'])
            area_id = self.areas.get(row['region'])
            family_situation_id = self.family_situations.get(row['situation_familiale'])
            
            # Build Person object according mapping and exclusion
            person_data = {}

            for csv_col, orm_attr in COLUMN_MAPPING.items():
                if csv_col in row and orm_attr not in ['child_number', 'caf_quotient']:
                    value = row[csv_col]

                    if orm_attr in ['sport_licence', 'smoker', 'french_nationality']:
                        person_data[orm_attr] = convert_bool(value)
                    elif orm_attr == 'account_creation_date':
                        person_data[orm_attr] = parse_date(value)
                    elif orm_attr == 'gender':
                        person_data[orm_attr] = str(value)
                    else:
                        person_data[orm_attr] = float(value) if pd.notna(value) else None

                # Special for two new cols
                person_data['child_number'] = int(row['nb_enfant']) if pd.notna(row['nb_enfant']) else None
                person_data['caf_quotient'] = float(row['quotient_caf']) if pd.notna(row['quotient_caf']) else None

                person_data['study_level_id'] = study_level_id
                person_data['area_id'] = area_id
                person_data['family_situation_id'] = family_situation_id


            person = Person(**person_data)
            
            self.session.add(person)
            
            # Commit par lots de 100 pour la performance
            if count % 100 == 0:
                self.session.commit()
        
        # Commit final
        self.session.commit()