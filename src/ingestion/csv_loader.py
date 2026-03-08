import pandas as pd
from pathlib import Path
from typing import Dict, List, Generator, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class CSVDataLoader:
    """
    Loader untuk CSV dengan chunk processing untuk dataset besar (1M+ records)
    """
    
    def __init__(self, file_path: str, chunk_size: int = 10000):
        self.file_path = Path(file_path)
        self.chunk_size = chunk_size
        self.total_rows = 0
        self.loaded_rows = 0
        
    def validate_file(self) -> bool:
        """Validasi file CSV ada dan dapat diakses"""
        if not self.file_path.exists():
            raise FileNotFoundError(f"File tidak ditemukan: {self.file_path}")
        
        if not self.file_path.is_file():
            raise ValueError(f"Bukan file: {self.file_path}")
        
        # Check file size
        file_size_mb = self.file_path.stat().st_size / 1024 / 1024
        logger.info(f"File size: {file_size_mb:.2f} MB")
        
        return True
    
    def get_total_rows(self) -> int:
        """Hitung total rows tanpa load semua data"""
        if self.total_rows == 0:
            self.total_rows = sum(1 for _ in open(self.file_path, 'r')) - 1  # Minus header
        return self.total_rows
    
    def load_chunk(self) -> Generator[pd.DataFrame, None, None]:
        """
        Load data dalam chunks untuk memory efficiency
        Yield satu chunk pada satu waktu
        """
        self.validate_file()
        
        logger.info(f"Starting chunked loading with chunk_size={self.chunk_size}")
        
        for chunk in pd.read_csv(
            self.file_path,
            chunksize=self.chunk_size,
            low_memory=False,
            dtype=str  # Load as string first, convert later
        ):
            self.loaded_rows += len(chunk)
            logger.info(f"Loaded chunk: {self.loaded_rows}/{self.get_total_rows()} rows")
            yield chunk
        
        logger.info(f"Chunk loading completed: {self.loaded_rows} total rows")
    
    def load_all(self) -> pd.DataFrame:
        """
        Load semua data (gunakan dengan hati-hati untuk dataset besar)
        """
        logger.info("Loading all data into memory")
        df = pd.read_csv(self.file_path, low_memory=False)
        logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def get_metadata(self) -> Dict:
        """Return metadata tentang data loading"""
        return {
            'file_path': str(self.file_path),
            'chunk_size': self.chunk_size,
            'total_rows': self.get_total_rows(),
            'loaded_rows': self.loaded_rows,
            'loaded_at': datetime.now().isoformat()
        }