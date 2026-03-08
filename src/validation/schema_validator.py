import yaml
import pandas as pd
from typing import Dict, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class SchemaValidator:
    """
    Validator untuk schema data kesejahteraan.
    Strict enforcement of data types and bounds.
    """
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.fields = self.config.get('fields', [])
        self.field_names = [f['name'] for f in self.fields]
        
    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validates a dataframe against the schema.
        Note: Modifies the DataFrame in-place by coercing types.
        """
        results = {
            'total_rows': len(df),
            'missing_columns': [],
            'type_errors': [],
            'value_errors': [],
            'missing_value_errors': [],
            'valid_rows': 0,
            'invalid_rows': 0,
            'validity_rate': 0.0
        }
        
        # 1. Check structural integrity
        missing_cols = [f['name'] for f in self.fields if f['name'] not in df.columns]
        if missing_cols:
            results['missing_columns'] = missing_cols
            results['status'] = 'FAIL'
            logger.error(f"Missing required columns: {missing_cols}")
            return results # Hard stop if structure is broken

        invalid_mask = pd.Series(False, index=df.index)
        
        for field in self.fields:
            field_name = field['name']
            
            # 2. Enforce Data Types
            if field['type'] in ['integer', 'float']:
                # Coerce to numeric. Anything unparseable becomes NaN.
                df[field_name] = pd.to_numeric(df[field_name], errors='coerce')
                
                # If a field is required, NaN is a violation.
                if field.get('required', True):
                    na_mask = df[field_name].isna()
                    if na_mask.any():
                        results['missing_value_errors'].append({
                            'field': field_name,
                            'count': int(na_mask.sum())
                        })
                        invalid_mask |= na_mask
            
            # 3. Enforce Bounds (Only check non-NaN values to avoid logic errors)
            if field['type'] in ['integer', 'float']:
                valid_numbers = df[field_name].notna()
                
                if 'min_value' in field:
                    min_mask = valid_numbers & (df[field_name] < field['min_value'])
                    if min_mask.any():
                        results['value_errors'].append({
                            'field': field_name,
                            'error': f"Below min {field['min_value']}",
                            'count': int(min_mask.sum())
                        })
                        invalid_mask |= min_mask
                
                if 'max_value' in field:
                    max_mask = valid_numbers & (df[field_name] > field['max_value'])
                    if max_mask.any():
                        results['value_errors'].append({
                            'field': field_name,
                            'error': f"Above max {field['max_value']}",
                            'count': int(max_mask.sum())
                        })
                        invalid_mask |= max_mask
            
            # 4. Enforce Allowed Values
            if 'allowed_values' in field:
                # ~df.isin() flags NaNs as invalid. Ensure we handle NaNs explicitly based on 'required' flag above.
                not_allowed_mask = df[field_name].notna() & ~df[field_name].isin(field['allowed_values'])
                if not_allowed_mask.any():
                    results['value_errors'].append({
                        'field': field_name,
                        'error': "Not in allowed values",
                        'count': int(not_allowed_mask.sum())
                    })
                    invalid_mask |= not_allowed_mask

        # Calculate final metrics
        results['invalid_rows'] = int(invalid_mask.sum())
        results['valid_rows'] = len(df) - results['invalid_rows']
        results['validity_rate'] = results['valid_rows'] / len(df) if len(df) > 0 else 0
        
        # Store the boolean mask so the ETL pipeline can actually filter the DataFrame
        results['valid_mask'] = ~invalid_mask 

        # A schema is a contract. ANY invalid rows means the validation "fails" to pass the whole chunk.
        # The ETL pipeline decides whether to drop the invalid rows or halt entirely.
        results['status'] = 'PASS' if results['invalid_rows'] == 0 else 'FAIL'
        
        if results['status'] == 'FAIL':
             logger.warning(f"Schema violations found. Validity: {results['validity_rate']*100:.2f}%")

        return results
    
    def get_invalid_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return records yang tidak valid untuk debugging"""
        # Implementasi similar dengan validate_dataframe
        # Return DataFrame dengan records invalid
        pass