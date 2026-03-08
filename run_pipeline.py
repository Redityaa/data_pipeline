import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Clean, top-level imports. Do not nest these inside functions.
from src.transformation.etl_pipeline import ETLPipeline
from src.transformation.rebalance_dataset import rebalance_undersample
from src.validation.validate_output import run_validation

# Load environment variables
load_dotenv()

# Setup logging properly. Everything must go through this, no print() statements.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Main execution function for the Data Preparation Pipeline"""
    
    logger.info("=" * 80)
    logger.info("🎯 DATA PREPARATION PIPELINE STARTED")
    logger.info("=" * 80)
    
    # 0. Environment Check
    if not os.getenv('DATA_SALT'):
        logger.warning("DATA_SALT not set! Using default (NOT FOR PRODUCTION). Set this immediately.")

    # ---------------------------------------------------------
    # PHASE 1: EXTRACTION, TRANSFORMATION, LOADING (ETL)
    # ---------------------------------------------------------
    logger.info(">>> STARTING PHASE 1: ETL <<<")
    pipeline = ETLPipeline(config_dir='configs')
    
    etl_result = pipeline.run(
        input_path='data/raw/dataset_kesejahteraan_jatim.csv',
        output_dir='data/processed',
        chunk_size=10000,
        skip_validation=False,
        skip_security=False
    )
    
    if etl_result.get('status') != 'success':
        logger.error(f"❌ ETL PHASE FAILED: {etl_result.get('error', 'Unknown Error')}")
        sys.exit(1) # Hard stop. Tell the OS we failed.

    logger.info(f"✅ ETL Completed. Processed {etl_result.get('total_records_processed', 0):,} records.")

    # ---------------------------------------------------------
    # PHASE 2: DATASET REBALANCING
    # ---------------------------------------------------------
    logger.info(">>> STARTING PHASE 2: DATASET REBALANCING <<<")
    try:
        final_counts = rebalance_undersample(
            input_path='data/processed/training_data.jsonl',
            output_path='data/processed/training_data_balanced.jsonl',
            target_per_class=100000 
        )
        logger.info(f"✅ Rebalancing Completed. Final distribution: {final_counts}")
    except Exception as e:
        logger.error(f"❌ REBALANCING PHASE FAILED: {e}")
        sys.exit(1) # Hard stop.

    # ---------------------------------------------------------
    # PHASE 3: OUTPUT VALIDATION
    # ---------------------------------------------------------
    logger.info(">>> STARTING PHASE 3: OUTPUT VALIDATION <<<")
    try:
        validation_results = run_validation(
            jsonl_file_path='data/processed/training_data_balanced.jsonl'
        )
        
        if validation_results['status'] != 'PASS':
            logger.error("❌ VALIDATION PHASE FAILED. Data is corrupt or incomplete.")
            # We log the specific failures so you can debug without running the script again
            for check in validation_results.get('checks', []):
                if check['status'] != 'PASS':
                    logger.error(f"Failed Check: {check}")
            sys.exit(1) # Hard stop. Prevent bad data from reaching the model.
            
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
    
    # Explicit successful exit
    sys.exit(0)

if __name__ == "__main__":
    main()