import json
import os
from pathlib import Path
from datetime import datetime

# Konfigurasi batas token 
MAX_TOKENS_PER_FIELD = 512  
MAX_TOTAL_TOKENS = 1024     

def estimate_tokens(text):
    """
    Estimasi jumlah token sederhana (rata-rata 4 karakter per token untuk Bahasa Indonesia).
    Untuk produksi, gunakan tokenizer asli dari model yang dituju (misal: tiktoken untuk OpenAI).
    """
    if not text:
        return 0
    # Estimasi kasar: panjang karakter / 4
    return len(str(text)) // 4

def run_validation(jsonl_file_path, check_tokens=True):
    """
    Fungsi validasi output pipeline.
    Mengecek keberadaan file, integritas JSON, distribusi kategori, dan validasi token.

    Args:
        jsonl_file_path: Path ke file JSONL yang divalidasi
        check_tokens: Boolean untuk mengaktifkan pengecekan token (default: True)
    """
    print("\n" + "=" * 80)
    print(f"🔍 STARTING OUTPUT VALIDATION: {jsonl_file_path}")
    if check_tokens:
        print(f"   🪙 Token Check Enabled (Max: {MAX_TOKENS_PER_FIELD}/field, {MAX_TOTAL_TOKENS}/record)")
    print("=" * 80)

    jsonl_path = Path(jsonl_file_path)
    validation_results = {
        'timestamp': datetime.now().isoformat(),
        'checks': [],
        'status': 'PASS',
        'kategori_distribution': {},
        'token_stats': {
            'enabled': check_tokens,
            'total_records_checked': 0,
            'records_exceeding_limit': 0,
            'max_tokens_found': 0,
            'avg_tokens_per_record': 0,
            'exceeded_records_sample': []
        }
    }

    # Konfigurasi kategori yang diharapkan
    expected_categories = [
        "Sangat Miskin", "Miskin", "Rentan Miskin",
        "Hampir Miskin", "Menengah Bawah", "Menengah Ke Atas"
    ]
    kategori_counts = {kat: 0 for kat in expected_categories}
    
    # 1. Cek keberadaan file
    if not jsonl_path.exists():
        print(f"   ❌ File not found: {jsonl_path}")
        validation_results['status'] = 'FAIL'
        return validation_results

    # 2. Validasi Konten (Single Pass / Sekali Baca)
    line_count = 0
    invalid_json = 0
    missing_fields_count = 0
    
    # Stats untuk token
    total_tokens = 0
    max_tokens_record = 0
    exceeding_records = []

    print(f"   📂 Analyzing records...")
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            line_count += 1
            try:
                record = json.loads(line)
                
                # Cek field utama
                required_fields = ['instruction', 'input', 'output', 'metadata']
                if not all(field in record for field in required_fields):
                    missing_fields_count += 1
                    continue

                # Cek struktur output JSON
                output_data = json.loads(record['output'])
                kat = output_data.get('kategori', 'Unknown')
                
                if kat in kategori_counts:
                    kategori_counts[kat] += 1
                else:
                    kategori_counts[kat] = kategori_counts.get(kat, 0) + 1
                
                # --- TOKEN VALIDATION ---
                if check_tokens:
                    instruction_tokens = estimate_tokens(record.get('instruction', ''))
                    input_tokens = estimate_tokens(record.get('input', ''))
                    output_tokens = estimate_tokens(record.get('output', ''))

                    total_record_tokens = instruction_tokens + input_tokens + output_tokens

                    # Update stats
                    total_tokens += total_record_tokens
                    if total_record_tokens > max_tokens_record:
                        max_tokens_record = total_record_tokens

                    # Cek apakah melebihi batas
                    field_violations = []
                    if instruction_tokens > MAX_TOKENS_PER_FIELD:
                        field_violations.append(f'instruction({instruction_tokens})')
                    if input_tokens > MAX_TOKENS_PER_FIELD:
                        field_violations.append(f'input({input_tokens})')
                    if output_tokens > MAX_TOKENS_PER_FIELD:
                        field_violations.append(f'output({output_tokens})')
                    if total_record_tokens > MAX_TOTAL_TOKENS:
                        field_violations.append(f'total({total_record_tokens})')

                    if field_violations:
                        validation_results['token_stats']['records_exceeding_limit'] += 1
                        if len(exceeding_records) < 5:  # Simpan max 5 sampel
                            exceeding_records.append({
                                'line_number': line_count,
                                'tokens': {
                                    'instruction': instruction_tokens,
                                    'input': input_tokens,
                                    'output': output_tokens,
                                    'total': total_record_tokens
                                },
                                'violations': field_violations
                            })

            except (json.JSONDecodeError, TypeError):
                invalid_json += 1

    # Hitung Ukuran File
    file_size_mb = jsonl_path.stat().st_size / 1024 / 1024

    # Hitung rata-rata token
    avg_tokens = total_tokens / line_count if line_count > 0 else 0

    # Simpan hasil ke dictionary
    validation_results['checks'].append({
        'name': 'integrity_check',
        'records': line_count,
        'invalid_json': invalid_json,
        'missing_fields': missing_fields_count,
        'size_mb': round(file_size_mb, 2)
    })
    validation_results['kategori_distribution'] = kategori_counts

    # Simpan stats token
    if check_tokens:
        validation_results['token_stats']['total_records_checked'] = line_count
        validation_results['token_stats']['max_tokens_found'] = max_tokens_record
        validation_results['token_stats']['avg_tokens_per_record'] = round(avg_tokens, 2)
        validation_results['token_stats']['exceeded_records_sample'] = exceeding_records

        if validation_results['token_stats']['records_exceeding_limit'] > 0:
            validation_results['checks'].append({
                'name': 'token_limit_check',
                'status': 'WARNING',
                'exceeding_count': validation_results['token_stats']['records_exceeding_limit'],
                'message': f"{validation_results['token_stats']['records_exceeding_limit']} records exceed token limits"
            })
            # warning saja karena bisa di-handle saat training
        else:
            validation_results['checks'].append({
                'name': 'token_limit_check',
                'status': 'PASS',
                'message': 'All records within token limits'
            })

    # 3. Tampilkan Laporan di Terminal
    print(f"   ✅ Total Records: {line_count:,}")
    print(f"   💾 File Size: {file_size_mb:.2f} MB")
    
    if invalid_json > 0 or missing_fields_count > 0:
        print(f"   ❌ Found {invalid_json} corrupt JSON and {missing_fields_count} incomplete records!")
        validation_results['status'] = 'FAIL'

    print("\n   📊 KATEGORI DISTRIBUTION:")
    print("   " + "-" * 70)
    for kat in expected_categories:
        count = kategori_counts.get(kat, 0)
        pct = (count / line_count * 100) if line_count > 0 else 0
        bar = "█" * int(pct / 2)
        print(f"   {kat:20s} | {count:>10,} | {pct:>6.2f}% | {bar}")
    
    # Tampilkan Token Stats jika diaktifkan
    if check_tokens:
        print("\n   🪙 TOKEN STATISTICS:")
        print("   " + "-" * 70)
        print(f"   Average Tokens/Record: {avg_tokens:.2f}")
        print(f"   Max Tokens Found:      {max_tokens_record}")
        print(f"   Limit (Total):         {MAX_TOTAL_TOKENS}")
        print(f"   Limit (Per Field):     {MAX_TOKENS_PER_FIELD}")

        exceeding_count = validation_results['token_stats']['records_exceeding_limit']
        if exceeding_count > 0:
            print(f"\n   ⚠️  WARNING: {exceeding_count} records exceed token limits!")
            print("   Sample exceeded records:")
            for sample in exceeding_records:
                print(f"     - Line {sample['line_number']}: {sample['violations']}")
        else:
            print(f"   ✅ All records are within token limits!")

    # 4. Validasi Log Files Tambahan
    other_checks = [
        ('pipeline_log.json', Path('data/processed/pipeline_log.json')),
        ('scrub_log.json', Path('data/processed/scrub_log.json')),
        ('pipeline.log', Path('data/logs/pipeline.log'))
    ]

    print("\n🔍 CHECKING LOG FILES:")
    for name, path in other_checks:
        status = "✅ EXISTS" if path.exists() else "⚠️  MISSING"
        print(f"   - {name:20s}: {status}")
        validation_results['checks'].append({'name': name, 'status': 'PASS' if path.exists() else 'WARNING'})

    # Simpan Laporan Akhir
    report_path = Path('data/processed/validation_report.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(validation_results, f, indent=2)

    if validation_results['status'] == 'PASS':
        print("\n" + "=" * 80)
        print("✅ ALL VALIDATION CHECKS PASSED! Data is ready for Phase 2.")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("⚠️  VALIDATION FAILED! Check the report for details.")
        print("=" * 80)

    return validation_results

if __name__ == "__main__":
    # Path default jika dijalankan manual
    run_validation('data/processed/training_data_balanced.jsonl', check_tokens=True)