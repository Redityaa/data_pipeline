from unittest import result

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """
    Checker untuk kualitas data (completeness, uniqueness, consistency)
    dengan kemampuan auto-cleaning untuk outliers
    """
    
    def __init__(self):
        self.thresholds = {
            'null_threshold': 0.01,  # 1% null dianggap kritis
            'duplicate_threshold': 0.001,  # 0.1% duplikat
            'outlier_threshold': 0.05  # 5% outlier
        }
    
        # Kolom binary yang tidak perlu di-check outlier
        self.binary_columns = [
            'motor', 'mobil', 'kulkas', 'tv', 'mesin_cuci',
            'tabungan', 'air_layak', 'sanitasi_layak',
            'overall_kelayakan_rumah'
        ]

        # Kolom continuous yang perlu di-clean outliers
        self.continuous_columns = [
            'pendapatan', 'pendapatan_perkapita', 'daya_listrik',
            'luas_rumah', 'rasio_luas', 'jumlah_anggota',
            'usia_kepala', 'wealth_index', 'desil'
        ]
    
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
    
    def check_numeric_ranges(self, df: pd.DataFrame, columns: List[str], auto_clean: bool = False) -> Dict:
        """Check nilai numeric dalam range yang wajar dengan opsi auto-cleaning"""
        outliers = {}
        cleaned_count = 0

        columns_to_check = [col for col in self.continuous_columns if col in df.columns]

        for col in columns_to_check:
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
                    'count': int(outlier_count),
                    'rate': float(outlier_count / len(df)),
                    'bounds': [float(lower_bound), float(upper_bound)]
                }
        
                # Auto-clean: clip outliers ke bounds
                if auto_clean:
                    # Cast ke float dulu untuk menghindari dtype incompatibility
                    if df[col].dtype in ['int64', 'int32']:
                        df[col] = df[col].astype(float)
                    df.loc[df[col] < lower_bound, col] = float(lower_bound)
                    df.loc[df[col] > upper_bound, col] = float(upper_bound)
                    cleaned_count += outlier_count

        result = {
            'columns_checked': columns_to_check,
            'outliers': outliers,
            'cleaned_count': cleaned_count if auto_clean else 0,
            'status': 'PASS' if len(outliers) == 0 else 'WARNING'
        }
        
        return result, df

    def generate_quality_report(self, df: pd.DataFrame, auto_clean: bool = False) -> Dict:
        """Generate laporan kualitas data lengkap dengan opsi auto-cleaning"""
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

        # Check numeric ranges dengan auto-clean jika diaktifkan
        numeric_result, df = self.check_numeric_ranges(df, numeric_columns, auto_clean=auto_clean)
        report['numeric_ranges'] = numeric_result
        
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
        
        return report, df

    def save_quality_report(self, report: Dict, output_path: str):
        """Save quality report to JSON file"""
        import json

        # Convert numpy types to Python native types for JSON serialization
        def convert_numpy(obj):
            if isinstance(obj, dict):
                return {k: convert_numpy(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy(i) for i in obj]
            elif isinstance(obj, (np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.float64, np.float32)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            else:
                return obj

        sanitized_report = convert_numpy(report)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(sanitized_report, f, indent=2, ensure_ascii=False)

        logger.info(f"Quality report saved to {output_path}")