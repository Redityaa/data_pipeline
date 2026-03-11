import json
import random
import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, Union

def rebalance_undersample(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    target_per_class: int = 100000,
    random_seed: int = 42
) -> Dict[str, int]:
    """
    Reads a ChatML JSONL dataset, undersamples majority classes, and saves a balanced dataset.
    """
    random.seed(random_seed)
    input_file = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    logging.info(f"Loading dataset from {input_file}")
    
    kategori_groups = defaultdict(list)
    total_loaded = 0
    
    with input_file.open('r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            try:
                record = json.loads(line)
                kategori = None
                
                # EKSTRAKSI CHATML: Cari pesan dari assistant
                messages = record.get('messages', [])
                for msg in messages:
                    if msg.get('role') == 'assistant':
                        assistant_content = msg.get('content', '{}')
                        # Parse string JSON di dalam content assistant
                        try:
                            content_data = json.loads(assistant_content)
                            kategori = content_data.get('kategori')
                        except json.JSONDecodeError:
                            logging.warning(f"Assistant content bukan JSON valid di baris {line_num}")
                        break # Berhenti mencari jika assistant sudah ditemukan
                
                # Jika tidak ada kategori yang ditemukan, lewati record ini
                if not kategori:
                    continue

                kategori_groups[kategori].append(record)
                total_loaded += 1
                
            except json.JSONDecodeError as e:
                logging.warning(f"Skipping malformed data at line {line_num}: {e}")

    if total_loaded == 0:
        raise ValueError("No valid records loaded. Pastikan format file sudah menggunakan ChatML yang benar.")

    logging.info(f"Successfully loaded {total_loaded:,} valid records.")

    balanced_records = []
    new_distribution = {}

    logging.info("Applying undersampling...")
    for kategori, recs in kategori_groups.items():
        if len(recs) > target_per_class:
            sampled = random.sample(recs, target_per_class)
            balanced_records.extend(sampled)
            new_distribution[kategori] = target_per_class
            logging.info(f"Undersampled {kategori}: {len(recs):,} -> {target_per_class:,}")
        else:
            balanced_records.extend(recs)
            new_distribution[kategori] = len(recs)
            logging.info(f"Kept {kategori}: {len(recs):,} (below target)")

    logging.info("Shuffling balanced dataset...")
    random.shuffle(balanced_records)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Saving {len(balanced_records):,} records to {output_file}")
    with output_file.open('w', encoding='utf-8') as f:
        for record in balanced_records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')

    logging.info("Rebalancing complete.")
    return new_distribution


if __name__ == "__main__":
    try:
        final_counts = rebalance_undersample(
            input_path='data/processed/training_data.jsonl',
            output_path='data/processed/training_data_balanced.jsonl',
            target_per_class=100000
        )
        print("\nFinal Distribution:", json.dumps(final_counts, indent=2))
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")