"""
cleaner.py
==========
Fase A: Pembersihan & Reduksi Fitur

Tugas:
  1. Drop kolom kode wilayah dan alamat (tidak ada nilai semantik / PII)
  2. Anonimisasi kolom PII (NKK → hash, PLN → mask, nama → redact)
  3. Handle missing values (fill sentinel)
  4. Log statistik missing value ke logs/missing_report.txt
"""

import hashlib
import logging
import os
import re
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Konstanta
# ---------------------------------------------------------------------------
COLS_TO_DROP = [
    "kode_provinsi",
    "kode_kabupaten_kota",
    "kode_kecamatan",
    "kode_kelurahan_desa",
    "alamat",
]

NUMERIC_FILL = -1       # Sentinel untuk kolom numerik yang hilang
STRING_FILL  = "Tidak Diketahui"


# ---------------------------------------------------------------------------
# Fungsi Anonimisasi
# ---------------------------------------------------------------------------
def _hash_nkk(val) -> str:
    """SHA-256 truncated 8 char, prefix NKK-."""
    if pd.isna(val):
        return "NKK-UNKNOWN"
    digest = hashlib.sha256(str(val).encode()).hexdigest()[:8].upper()
    return f"NKK-{digest}"


def _mask_pln(val) -> str:
    """Mask ID PLN → PLN-XXXXXXXX."""
    if pd.isna(val):
        return "PLN-UNKNOWN"
    raw = str(int(val)) if isinstance(val, float) else str(val)
    masked = "X" * max(len(raw) - 4, 4) + raw[-4:]
    return f"PLN-{masked}"


def _redact_name(_val) -> str:
    return "[NAMA DISEMBUNYIKAN]"


# ---------------------------------------------------------------------------
# Fungsi Utama
# ---------------------------------------------------------------------------
def clean_chunk(
    df: pd.DataFrame,
    missing_log: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Bersihkan satu chunk DataFrame.

    Parameters
    ----------
    df          : chunk DataFrame dari pembacaan CSV
    missing_log : dict akumulasi {kolom: jumlah_missing} untuk laporan akhir

    Returns
    -------
    DataFrame yang sudah dibersihkan
    """
    df = df.copy()

    # ------------------------------------------------------------------ #
    # 1. Catat missing values sebelum drop
    # ------------------------------------------------------------------ #
    if missing_log is not None:
        for col in df.columns:
            n_miss = int(df[col].isna().sum())
            if n_miss > 0:
                missing_log[col] = missing_log.get(col, 0) + n_miss

    # ------------------------------------------------------------------ #
    # 2. Drop kolom tidak berguna / PII lokasi
    # ------------------------------------------------------------------ #
    cols_to_drop_actual = [c for c in COLS_TO_DROP if c in df.columns]
    df.drop(columns=cols_to_drop_actual, inplace=True)

    # ------------------------------------------------------------------ #
    # 3. Anonimisasi PII
    # ------------------------------------------------------------------ #
    if "nomor_kartu_keluarga" in df.columns:
        df["nomor_kartu_keluarga"] = df["nomor_kartu_keluarga"].apply(_hash_nkk)

    if "id_pelanggan_pln" in df.columns:
        df["id_pelanggan_pln"] = df["id_pelanggan_pln"].apply(_mask_pln)

    if "nama_anggota_keluarga" in df.columns:
        df["nama_anggota_keluarga"] = df["nama_anggota_keluarga"].apply(_redact_name)

    # ------------------------------------------------------------------ #
    # 4. Handle missing values
    # ------------------------------------------------------------------ #
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].fillna(STRING_FILL)
        else:
            df[col] = df[col].fillna(NUMERIC_FILL)

    return df


def write_missing_report(missing_log: dict, log_dir: str) -> None:
    """Tulis laporan missing value ke file teks."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    report_path = os.path.join(log_dir, "missing_report.txt")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== LAPORAN MISSING VALUES (PER KOLOM) ===\n\n")
        if not missing_log:
            f.write("Tidak ada missing values yang ditemukan.\n")
        else:
            for col, count in sorted(missing_log.items(), key=lambda x: -x[1]):
                f.write(f"  {col:<45}: {count:>8} baris\n")

    logger.info(f"Missing report ditulis ke {report_path}")
