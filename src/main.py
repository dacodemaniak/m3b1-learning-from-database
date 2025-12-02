from data_orm.infrastructure.database import Database
from data_orm.infrastructure.services.data_loader import CSVDataLoader
from data_orm.config.config import DATABASE_URL
from data_orm.config.logger import get_logger
import uvicorn
from data_orm.api.main import app
from data_orm.infrastructure.pipeline_orchestrator import PipelineOrchestrator


logger = get_logger()

HOST="0.0.0.0"
PORT=8088

def start_api():
    base_url = f"http://127.0.0.1:{PORT}"
    logger.info("API launching...")
    logger.info(f"Listen on {base_url}")
    logger.info(f"Swagger on {base_url}/docs")
    logger.info(f"ReDoc {base_url}/redoc")

    uvicorn.run(app, host=HOST, port=PORT)

    logger.info("API was launched")

def load_data():
    # Initialisation de la base de données
    db = Database(DATABASE_URL)
    
    # Création des tables
    db.create_tables()
    logger.info("Tables créées avec succès!")
    
    # Chargement des données
    csv_path = "csv/data-all-684bf775c031b265646213-5-692db467bf949154256294.csv"
    
    with db.session_scope() as session:
        loader = CSVDataLoader(session)
        logger.info("Chargement des niveaux d'étude...")
        study_levels = loader.load_study_levels(csv_path)
        logger.info(f"{len(study_levels)} niveaux d'étude chargés")
        
        logger.info("Chargement des régions...")
        areas = loader.load_areas(csv_path)
        logger.info(f"{len(areas)} régions chargées")
        
        logger.info("Chargement des situations familiales...")
        family_situations = loader.load_family_situations(csv_path)
        logger.info(f"{len(family_situations)} situations familiales chargées")
        
        logger.info("Chargement des personnes...")
        loader.load_persons(csv_path)
        logger.info("Données chargées avec succès!")

def intiate_pipeline():
    config = {
        'input_file': 'from_database',
        'output_file': 'data_source/processed_data.csv',
        'log_file': 'logs/pipeline.log',
        'anonymization': {
            'strategy': 'hash',
            'explicit_sensitive_columns': ['nom', 'prenom']
        },
        'cleaning': {
            'missing_values_strategy': 'auto',
            'outlier_removal_method': 'iqr'
        },
        'relevance_filter': {
            'strict_mode': True,
            'allowed_columns': [
                'nom',
                'prenom',
                'revenu_estime_mois',
                'loyer_mensuel',
                'montant_pret'
            ]
        },
        'normalization': {
            'method': 'minmax'
        }
    }


    orchestrator = PipelineOrchestrator(config)

    logger.info("🚀 Starting compliant data processing pipeline...")
    logger.info("📏 Sensitive datas extracted from database")

    processed_data = orchestrator.run_pipeline()
    
    print("Pipeline execution completed!")
    print(f"Processed data shape: {processed_data.shape}")

    print(f"🎯 Final columns: {list(processed_data.columns)}")

if __name__ == "__main__":
    # load_data()
    start_api()