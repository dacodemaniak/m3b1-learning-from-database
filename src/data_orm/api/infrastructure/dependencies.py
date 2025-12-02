from sqlalchemy.orm import Session
from typing import Generator
# Importe les classes Database et la configuration
from data_orm.infrastructure.database import Database
from data_orm.config.config import DATABASE_URL 

# --- INITIALISATION GLOBALE DE LA DB ---
# L'instance unique de la base de données est créée ici, dans un fichier de "bas niveau".
global_db_instance = Database(DATABASE_URL)
global_db_instance.create_tables()

# --- DÉPENDANCE FASTAPI ---
def get_db() -> Generator[Session, None, None]:
    """Dépendance qui fournit et ferme la session de base de données pour chaque requête."""
    db = global_db_instance.get_session() 
    try:
        yield db
    finally:
        # La fermeture de la session est essentielle
        db.close()