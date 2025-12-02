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