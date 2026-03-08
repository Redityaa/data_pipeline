import hashlib
import os
from typing import Dict, List, Any
from datetime import datetime
import logging
import yaml
import json

logger = logging.getLogger(__name__)

class PIIScrubber:
    """
    PII Scrubber untuk keamanan data - UU PDP Compliance
    """
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.pii_fields = self.config.get('pii_fields', [])
        self.salt = self._get_secure_salt()
        self.scrub_log = []
        
    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _get_secure_salt(self) -> str:
        salt = os.getenv('DATA_SALT')
        if not salt:
            logger.warning("DATA_SALT not found in environment")
            salt = 'default_salt_change_in_production'
        return salt
    
    def hash_field(self, value: str, field_name: str) -> str:
        """Hash field dengan SHA256 + salt"""
        if value is None or value == '':
            return None
        salted = f"{self.salt}_{field_name}_{str(value)}"
        return hashlib.sha256(salted.encode()).hexdigest()[:16]
    
    def scrub_dataframe(self, df) -> Any:
        """
        Proses scrubbing pada DataFrame
        
        Args:
            df: pandas DataFrame
            
        Returns:
            DataFrame dengan data yang sudah di-scrub
        """
        import pandas as pd
        scrubbed_df = df.copy()
        
        actions_taken = []
        
        for pii_config in self.pii_fields:
            field = pii_config['field']
            action = pii_config['action']
            
            if field not in scrubbed_df.columns:
                continue
            
            if action == 'hash':
                scrubbed_df[f"{field}_hashed"] = scrubbed_df[field].apply(
                    lambda x: self.hash_field(x, field) if pd.notna(x) else None
                )
                scrubbed_df.drop(columns=[field], inplace=True)
                actions_taken.append(f"{field}: hashed")
                
            elif action == 'remove':
                scrubbed_df.drop(columns=[field], inplace=True)
                actions_taken.append(f"{field}: removed")
                
            elif action == 'keep':
                actions_taken.append(f"{field}: kept ({pii_config.get('reason', '')})")
        
        # Add security metadata
        scrubbed_df['_security_metadata'] = json.dumps({
            'scrubbed_at': datetime.now().isoformat(),
            'scrubber_version': '1.0.0',
            'actions_taken': actions_taken
        })
        
        self.scrub_log.append({
            'timestamp': datetime.now().isoformat(),
            'records_processed': len(scrubbed_df),
            'actions': actions_taken
        })
        
        logger.info(f"PII scrubbing completed: {len(actions_taken)} actions")
        
        return scrubbed_df
    
    def save_scrub_log(self, output_path: str):
        """Save scrub log untuk audit trail"""
        with open(output_path, 'w') as f:
            json.dump(self.scrub_log, f, indent=2)
        logger.info(f"Scrub log saved to {output_path}")