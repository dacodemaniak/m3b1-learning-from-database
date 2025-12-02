from loguru import logger
import sys

# Supprime le logger par défaut pour éviter double log
logger.remove()

# Ajouter un handler console avec format lisible
logger.add(sys.stdout, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

# Ajouter un fichier journal tournant (rotation toutes les 10MB)
logger.add("logs/data_processing_{time:YYYY-MM-DD}.log", rotation="10 MB", retention="10 days", level="DEBUG", compression="zip")

# Fonction utilitaire pour récupération logger
def get_logger():
    return logger
