import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Clean, top-level imports.
from src.transformation.etl_pipeline import ETLPipeline
from src.transformation.rebalance_dataset import rebalance_undersample
from src.validation.validate_output import run_validation

# Load environment variables
load_dotenv()

# AMANKAN DIREKTORI LOG SEBELUM LOGGING DIMULAI
log_dir = Path('data/logs')
log_dir.mkdir(parents=True, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 80)
    logger.info("🎯 DATA PREPARATION PIPELINE STARTED")
    logger.info("=" * 80)
    
    # 0. Environment Check
    if not os.getenv('DATA_SALT'):
        logger.warning("DATA_SALT not set! Using default (NOT FOR PRODUCTION). Set this immediately.")

    # DEFINISIKAN PATH DI SATU TEMPAT AGAR TIDAK TERPUTUS
    RAW_DATA_PATH = 'data/raw/dataset_kesejahteraan_jatim.csv'
    PROCESSED_DIR = 'data/processed'
    BASE_JSONL_PATH = f'{PROCESSED_DIR}/training_data.jsonl'
    BALANCED_JSONL_PATH = f'{PROCESSED_DIR}/training_data_balanced.jsonl'

    # ---------------------------------------------------------
    # PHASE 1: EXTRACTION, TRANSFORMATION, LOADING (ETL)
    # ---------------------------------------------------------
    logger.info(">>> STARTING PHASE 1: ETL <<<")
    pipeline = ETLPipeline(config_dir='configs')
    
    etl_result = pipeline.run(
        input_path=RAW_DATA_PATH,
        output_dir=PROCESSED_DIR,
        chunk_size=10000,
        skip_validation=False,
        skip_security=False
    )
    
    if etl_result.get('status') != 'success':
        logger.error(f"❌ ETL PHASE FAILED: {etl_result.get('error', 'Unknown Error')}")
        sys.exit(1)

    logger.info(f"✅ ETL Completed. Processed {etl_result.get('total_records_processed', 0):,} records.")

    # ---------------------------------------------------------
    # PHASE 2: DATASET REBALANCING
    # ---------------------------------------------------------
    logger.info(">>> STARTING PHASE 2: DATASET REBALANCING <<<")
    
    if not Path(BASE_JSONL_PATH).exists():
        logger.error(f"❌ REBALANCING FAILED: Input file {BASE_JSONL_PATH} missing. Did ETL fail silently?")
        sys.exit(1)
        
    try:
        final_counts = rebalance_undersample(
            input_path=BASE_JSONL_PATH,
            output_path=BALANCED_JSONL_PATH,
            target_per_class=100000 
        )
        logger.info(f"✅ Rebalancing Completed. Final distribution: {final_counts}")
    except Exception as e:
        logger.error(f"❌ REBALANCING PHASE FAILED: {e}")
        sys.exit(1)

    # ---------------------------------------------------------
    # PHASE 3: OUTPUT VALIDATION
    # ---------------------------------------------------------
    logger.info(">>> STARTING PHASE 3: OUTPUT VALIDATION <<<")
    try:
        validation_results = run_validation(
            jsonl_file_path=BALANCED_JSONL_PATH
        )
        
        if validation_results['status'] != 'PASS':
            logger.error("❌ VALIDATION PHASE FAILED. Data is corrupt or incomplete.")
            for check in validation_results.get('checks', []):
                if check['status'] != 'PASS':
                    logger.error(f"Failed Check: {check}")
            sys.exit(1)
            
        logger.info("✅ Validation Completed. Output data is clean and ready for modeling.")
    except Exception as e:
        logger.error(f"❌ VALIDATION PHASE CRASHED: {e}")
        sys.exit(1)

    # ---------------------------------------------------------
    # PIPELINE SUMMARY
    # ---------------------------------------------------------
    logger.info("=" * 80)
    logger.info("✅ ALL PIPELINE PHASES COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info("Ready for Model Training Phase.")
    
    sys.exit(0)

if __name__ == "__main__":
    main()