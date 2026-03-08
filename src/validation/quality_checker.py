import pandas as pd
import numpy as np
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """
    Checker untuk kualitas data (completeness, uniqueness, consistency)
    """
    
    def __init__(self):
        self.thresholds = {
            'null_threshold': 0.01,  # 1% null dianggap kritis
            'duplicate_threshold': 0.001,  # 0.1% duplikat
            'outlier_threshold': 0.05  # 5% outlier
        }
    
    def check_completeness(self, df: pd.DataFrame) -> Dict:
        """Check kelengkapan data"""
        total_cells = df.size
        null_cells = df.isnull().sum().sum()
        null_rate = null_cells / total_cells if total_cells > 0 else 0
        
        per_column = (df.isnull().sum() / len(df)).to_dict()
        critical_columns = [
            col for col, rate in per_column.items() 
            if rate > self.thresholds['null_threshold']
        ]
        
        return {
            'overall_null_rate': null_rate,
            'per_column_null_rate': per_column,
            'critical_columns': critical_columns,
            'status': 'PASS' if null_rate < self.thresholds['null_threshold'] else 'FAIL'
        }
    
    def check_duplicates(self, df: pd.DataFrame) -> Dict:
        """Check duplikasi data"""
        total = len(df)
        duplicates = df.duplicated().sum()
        duplicate_rate = duplicates / total if total > 0 else 0
        
        return {
            'total_records': total,
            'duplicate_records': duplicates,
            'duplicate_rate': duplicate_rate,
            'status': 'PASS' if duplicate_rate < self.thresholds['duplicate_threshold'] else 'FAIL'
        }
    
    def check_numeric_ranges(self, df: pd.DataFrame, columns: List[str]) -> Dict:
        """Check nilai numeric dalam range yang wajar"""
        outliers = {}
        
        for col in columns:
            if col not in df.columns:
                continue
            
            if df[col].dtype not in ['int64', 'float64']:
                continue
            
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
            outlier_count = outlier_mask.sum()
            
            if outlier_count > 0:
                outliers[col] = {
                    'count': outlier_count,
                    'rate': outlier_count / len(df),
                    'bounds': [lower_bound, upper_bound]
                }
        
        return {
            'columns_checked': columns,
            'outliers': outliers,
            'status': 'PASS' if len(outliers) == 0 else 'WARNING'
        }
    
    def generate_quality_report(self, df: pd.DataFrame) -> Dict:
        """Generate laporan kualitas data lengkap"""
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        report = {
            'dataset_info': {
                'rows': len(df),
                'columns': len(df.columns),
                'memory_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            },
            'completeness': self.check_completeness(df),
            'duplicates': self.check_duplicates(df),
            'numeric_ranges': self.check_numeric_ranges(df, numeric_columns)
        }
        
        # Overall status
        statuses = [
            report['completeness']['status'],
            report['duplicates']['status'],
            report['numeric_ranges']['status']
        ]
        
        if 'FAIL' in statuses:
            report['overall_status'] = 'FAIL'
        elif 'WARNING' in statuses:
            report['overall_status'] = 'WARNING'
        else:
            report['overall_status'] = 'PASS'
        
        return report