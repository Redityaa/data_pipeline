import json
import os
from pathlib import Path
from datetime import datetime

def run_validation(jsonl_file_path):
    """
    Fungsi validasi output pipeline.
    Mengecek keberadaan file, integritas JSON, dan distribusi kategori dalam satu kali baca.
    """
    print("\n" + "=" * 80)
    print(f"🔍 STARTING OUTPUT VALIDATION: {jsonl_file_path}")
    print("=" * 80)

    jsonl_path = Path(jsonl_file_path)
    validation_results = {
        'timestamp': datetime.now().isoformat(),
        'checks': [],
        'status': 'PASS',
        'kategori_distribution': {}
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

            except (json.JSONDecodeError, TypeError):
                invalid_json += 1

    # Hitung Ukuran File
    file_size_mb = jsonl_path.stat().st_size / 1024 / 1024

    # Simpan hasil ke dictionary
    validation_results['checks'].append({
        'name': 'integrity_check',
        'records': line_count,
        'invalid_json': invalid_json,
        'missing_fields': missing_fields_count,
        'size_mb': round(file_size_mb, 2)
    })
    validation_results['kategori_distribution'] = kategori_counts

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
    run_validation('data/processed/training_data_balanced.jsonl')