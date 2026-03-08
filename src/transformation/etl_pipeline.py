from pathlib import Path
import json
import logging
from datetime import datetime
from typing import Dict, Any

from ..ingestion.csv_loader import CSVDataLoader
from ..validation.schema_validator import SchemaValidator
from ..validation.quality_checker import DataQualityChecker
from ..security.pii_scrubber import PIIScrubber
from ..labeling.instruction_formatter import InstructionFormatter

logger = logging.getLogger(__name__)

class ETLPipeline:
    """
    Main ETL Pipeline untuk data kesejahteraan
    Mendukung chunk processing untuk dataset besar (1M+ records)
    """
    
    def __init__(self, config_dir: str):
        self.config_dir = Path(config_dir)
        
        # Initialize components
        self.schema_validator = SchemaValidator(
            str(self.config_dir / 'data_schema.yaml')
        )
        self.pii_scrubber = PIIScrubber(
            str(self.config_dir / 'security_config.yaml')
        )
        self.instruction_formatter = InstructionFormatter(
            str(self.config_dir / 'labeling_config.yaml')
        )
        self.quality_checker = DataQualityChecker()
        
        self.pipeline_log = []
        
    def run(self, 
            input_path: str,
            output_dir: str,
            chunk_size: int = 10000,
            skip_validation: bool = False,
            skip_security: bool = False
           ) -> Dict[str, Any]:
        """
        Execute full ETL pipeline with chunk processing and continuous appending.
        """
        import pandas as pd # Ensure pandas is imported at the top of your file
        
        start_time = datetime.now()
        logger.info(f"Starting ETL pipeline at {start_time}")
        
        pipeline_result = {
            'start_time': start_time.isoformat(),
            'status': 'running',
            'steps': {},
            'total_records_processed': 0
        }
        
        try:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            final_output_file = output_path / 'training_data.jsonl'
            
            # Wipe existing file to prevent appending to old runs
            if final_output_file.exists():
                final_output_file.unlink()

            loader = CSVDataLoader(input_path, chunk_size=chunk_size)
            chunk_count = 0
            
            for chunk_df in loader.load_chunk():
                chunk_count += 1
                logger.info(f"Processing chunk {chunk_count} ({len(chunk_df)} records)")
                
                # ---------------------------------------------------------
                # STEP 0: SANITIZATION (The fix for your TypeError)
                # ---------------------------------------------------------
                # Strip commas from all object (string) columns and force numeric coercion
                # where applicable. 
                for col in chunk_df.select_dtypes(include=['object']).columns:
                    # Bersihkan koma jika ada
                    if chunk_df[col].astype(str).str.contains(',').any():
                        chunk_df[col] = chunk_df[col].astype(str).str.replace(',', '', regex=False)
                    
                    # Konversi secara eksplisit.
                    # Jika seluruh kolom bisa menjadi angka, jadikan angka. 
                    # Jika ada teks (seperti kolom Nama/Alamat), tangkap errornya dan biarkan tetap sebagai string.
                    try:
                        chunk_df[col] = pd.to_numeric(chunk_df[col])
                    except (ValueError, TypeError):
                        pass

                # ---------------------------------------------------------
                # STEP 1: VALIDATION
                # ---------------------------------------------------------
                if not skip_validation:
                    validation_result = self.schema_validator.validate_dataframe(chunk_df)
                    
                    if validation_result['status'] == 'FAIL':
                        # Log the errors so you know what is failing
                        logger.warning(f"Chunk {chunk_count} had {validation_result['invalid_rows']} invalid rows.")
                        
                        # Use the mask to drop the bad rows from the chunk!
                        chunk_df = chunk_df[validation_result['valid_mask']].copy()
                        
                        # If the whole chunk was garbage, skip the rest of the steps
                        if chunk_df.empty:
                             logger.warning(f"Chunk {chunk_count} is empty after validation. Skipping.")
                             continue
                
                # ---------------------------------------------------------
                # STEP 2 & 3: QUALITY & SECURITY
                # ---------------------------------------------------------
                self.quality_checker.generate_quality_report(chunk_df)
                
                if not skip_security:
                    chunk_df = self.pii_scrubber.scrub_dataframe(chunk_df)
                
                # ---------------------------------------------------------
                # STEP 4: FORMAT AND CONTINUOUS SAVE (The fix for your data loss)
                # ---------------------------------------------------------
                formatted_records = self.instruction_formatter.format_dataframe(chunk_df)
                
                # Append directly to the final file to keep memory usage flat
                with open(final_output_file, 'a', encoding='utf-8') as f:
                    for record in formatted_records:
                        f.write(json.dumps(record, ensure_ascii=False) + '\n')
                
                pipeline_result['total_records_processed'] += len(chunk_df)

            # Save security logs
            if not skip_security:
                self.pii_scrubber.save_scrub_log(str(output_path / 'scrub_log.json'))
            
            # Wrap up
            end_time = datetime.now()
            pipeline_result['status'] = 'success'
            pipeline_result['end_time'] = end_time.isoformat()
            pipeline_result['duration_formatted'] = str(end_time - start_time)
            
            logger.info(f"ETL completed successfully. Total processed: {pipeline_result['total_records_processed']}")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            pipeline_result['status'] = 'failed'
            pipeline_result['error'] = str(e)
            
        finally:
            self._save_pipeline_log(output_path, pipeline_result)
            
        return pipeline_result
    
    def _save_pipeline_log(self, output_path: Path, result: Dict):
        """Save pipeline execution log"""
        log_file = output_path / 'pipeline_log.json'
        with open(log_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        logger.info(f"Pipeline log saved to {log_file}")