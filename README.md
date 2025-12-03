# Data integration from database

Project is based on the projet "data" : [p3-data](https://github.com/dacodemaniak/p3-data-processor)

## Improvements

- sqlalchemy ORM implementation : Entities and repositories are managed by ORM,
- alembic migrations,
- raw sql implementation to only expose columns needed
- docker postgresql,
- FastAPI deployement
- SQL to Dataframe mapping

## How to

Create a migration script

`alembic revision --autogenerate -m "Your message"`

Run the migration script

`alembic upgrade head`

## Notes :

- Some columns from new file was excluded, just update config.py to configure those columns :

`EXCLUDE_COLUMNS: List[str] = ['orientation_sexuelle']`

- Mapping is self managed, based from a Dict :

```python
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
```

- Utils was created to parse or transform flat datas :

```python
from datetime import datetime
from typing import Optional

def convert_bool(value: str) -> bool:
    """Convertit une chaîne en booléen ('oui' devient True, autre chose devient False)"""
    if isinstance(value, str):
        # La vérification insensible à la casse et stricte à 'oui' est la meilleure pratique
        return value.strip().lower() == 'oui'
    # Gère les autres types (par exemple, 0/1 ou True/False qui pourraient être passés directement)
    return bool(value)

def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse une date au format YYYY-MM-DD.
    Retourne None si la chaîne est vide, NaN, ou le format est incorrect.
    """
    if not isinstance(date_str, str) or not date_str.strip():
        return None
    try:
        # Assurez-vous d'utiliser une méthode robuste pour les formats de date
        return datetime.strptime(date_str, '%Y-%m-%d')
    except (ValueError, TypeError):
        # Vous pouvez logguer l'erreur ici si nécessaire
        return None
```

## Final thaughts

- Improve config to avoid to update a sensible file,
- Automate mapping, using "lambdas" avoiding to hard code conversions,
- Integrate MLFlow to manage learning metrics,
- Create an IHM to view original datas and let user decide what to keep, what to anonymize, what to exclude,
- Create a workflow to automate processor_chain needed (i.e Pick individual processors and chain it as user needs)
