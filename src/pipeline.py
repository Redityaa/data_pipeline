"""
pipeline.py
===========
Entry point utama Pipeline ETL: DTSEN CSV → ChatML JSONL

Alur kerja:
  CSV (chunk 10.000) → Fase A (clean) → Fase B (tekstualisasi) → Fase C (JSONL)

Jalankan:
  python src/pipeline.py
  python src/pipeline.py --config config.yaml
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import yaml
from tqdm import tqdm

# Pastikan root proyek ada di sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.cleaner       import clean_chunk, write_missing_report
from src.tekstualisasi import tekstualisasi_chunk
from src.chatml        import write_chunk_to_jsonl


# ---------------------------------------------------------------------------
# Setup Logging
# ---------------------------------------------------------------------------
def setup_logging(log_dir: str) -> logging.Logger:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = os.path.join(log_dir, "pipeline.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# Hitung total baris CSV (untuk progress bar akurat)
# ---------------------------------------------------------------------------
def count_rows(csv_path: str, encoding: str = "utf-8") -> int:
    """Hitung baris data (tidak termasuk header) dengan efisien."""
    count = 0
    with open(csv_path, "r", encoding=encoding, errors="replace") as f:
        next(f)  # skip header
        for _ in f:
            count += 1
    return count


# ---------------------------------------------------------------------------
# Pipeline Utama
# ---------------------------------------------------------------------------
def run_pipeline(config: dict) -> None:
    paths      = config["paths"]
    proc       = config["processing"]
    system_prompt = config["chatml"]["system_prompt"]

    input_csv   = ROOT / paths["input_csv"]
    output_file = ROOT / paths["output_file"]
    log_dir     = str(ROOT / paths["log_dir"])
    chunk_size  = int(proc["chunk_size"])
    encoding    = proc.get("csv_encoding", "utf-8")

    logger = setup_logging(log_dir)
    logger.info("=" * 60)
    logger.info("Pipeline DTSEN → ChatML/JSONL mulai")
    logger.info(f"  Input : {input_csv}")
    logger.info(f"  Output: {output_file}")
    logger.info(f"  Chunk : {chunk_size:,} baris")
    logger.info("=" * 60)

    # Pastikan direktori output ada
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Hitung total baris untuk progress bar
    logger.info("Menghitung total baris CSV …")
    t0 = time.time()
    total_rows = count_rows(str(input_csv), encoding=encoding)
    logger.info(f"Total baris data: {total_rows:,} ({time.time()-t0:.1f}s)")

    total_written = 0
    missing_log   = {}
    start_time    = time.time()

    # Buka output file (overwrite jika sudah ada)
    with open(output_file, "w", encoding="utf-8") as out_f:

        reader = pd.read_csv(
            input_csv,
            chunksize=chunk_size,
            encoding=encoding,
            encoding_errors="replace",
            low_memory=False,
        )

        with tqdm(
            total=total_rows,
            unit="baris",
            desc="Memproses",
            ncols=80,
            dynamic_ncols=True,
        ) as pbar:

            for chunk_idx, chunk in enumerate(reader):
                chunk_len = len(chunk)

                try:
                    # ── Fase A ──────────────────────────────────────────── #
                    cleaned = clean_chunk(chunk, missing_log=missing_log)

                    # ── Fase B ──────────────────────────────────────────── #
                    tekstual = tekstualisasi_chunk(cleaned)

                    # ── Fase C ──────────────────────────────────────────── #
                    written = write_chunk_to_jsonl(tekstual, out_f, system_prompt)
                    total_written += written

                except Exception as exc:
                    logger.error(
                        f"Error pada chunk {chunk_idx} "
                        f"(baris ~{chunk_idx * chunk_size}): {exc}",
                        exc_info=True,
                    )
                    # Lanjutkan ke chunk berikutnya (jangan hentikan pipeline)

                pbar.update(chunk_len)

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Pipeline selesai dalam {elapsed:.1f} detik")
    logger.info(f"Total record ditulis : {total_written:,}")
    logger.info(f"Output               : {output_file}")

    # Tulis laporan missing values
    write_missing_report(missing_log, log_dir)

    if missing_log:
        logger.warning(
            f"Ditemukan missing values pada {len(missing_log)} kolom. "
            f"Lihat {log_dir}/missing_report.txt"
        )
    else:
        logger.info("Tidak ada missing values yang ditemukan.")

    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL DTSEN → ChatML/JSONL"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path ke file konfigurasi YAML (default: config.yaml)",
    )
    args = parser.parse_args()

    config_path = ROOT / args.config
    if not config_path.exists():
        print(f"[ERROR] Config tidak ditemukan: {config_path}")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    run_pipeline(config)


if __name__ == "__main__":
    main()
