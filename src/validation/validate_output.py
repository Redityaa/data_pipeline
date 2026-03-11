import json
import os
from pathlib import Path
from datetime import datetime

# Konfigurasi batas token 
MAX_TOKENS_PER_FIELD = 512  
MAX_TOTAL_TOKENS = 1024     

def estimate_tokens(text):
    """Estimasi jumlah token sederhana (rata-rata 4 karakter per token)."""
    if not text:
        return 0
    return len(str(text)) // 4

def run_validation(jsonl_file_path, check_tokens=True):
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

    expected_categories = [
        "Sangat Miskin", "Miskin", "Rentan Miskin",
        "Hampir Miskin", "Menengah Bawah", "Menengah Ke Atas"
    ]
    kategori_counts = {kat: 0 for kat in expected_categories}
    
    if not jsonl_path.exists():
        print(f"   ❌ File not found: {jsonl_path}")
        validation_results['status'] = 'FAIL'
        return validation_results

    line_count = 0
    invalid_json = 0
    missing_fields_count = 0
    
    total_tokens = 0
    max_tokens_record = 0
    exceeding_records = []

    print(f"   📂 Analyzing records (ChatML format)...")
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            line_count += 1
            try:
                record = json.loads(line)
                
                # VALIDASI STRUKTUR CHATML
                if 'messages' not in record or not isinstance(record['messages'], list):
                    missing_fields_count += 1
                    continue

                messages = record['messages']
                
                # Ekstrak konten berdasarkan role
                system_content = next((m.get('content', '') for m in messages if m.get('role') == 'system'), '')
                user_content = next((m.get('content', '') for m in messages if m.get('role') == 'user'), '')
                assistant_content = next((m.get('content', '') for m in messages if m.get('role') == 'assistant'), '')
                
                if not assistant_content:
                    missing_fields_count += 1
                    continue

                # VALIDASI KATEGORI (Dari dalam output JSON assistant)
                try:
                    output_data = json.loads(assistant_content)
                    kat = output_data.get('kategori', 'Unknown')
                except json.JSONDecodeError:
                    kat = 'Unknown'
                    invalid_json += 1
                
                kategori_counts[kat] = kategori_counts.get(kat, 0) + 1
                
                # TOKEN VALIDATION
                if check_tokens:
                    sys_tokens = estimate_tokens(system_content)
                    usr_tokens = estimate_tokens(user_content)
                    ast_tokens = estimate_tokens(assistant_content)

                    total_record_tokens = sys_tokens + usr_tokens + ast_tokens

                    total_tokens += total_record_tokens
                    if total_record_tokens > max_tokens_record:
                        max_tokens_record = total_record_tokens

                    field_violations = []
                    if sys_tokens > MAX_TOKENS_PER_FIELD:
                        field_violations.append(f'system({sys_tokens})')
                    if usr_tokens > MAX_TOKENS_PER_FIELD:
                        field_violations.append(f'user({usr_tokens})')
                    if ast_tokens > MAX_TOKENS_PER_FIELD:
                        field_violations.append(f'assistant({ast_tokens})')
                    if total_record_tokens > MAX_TOTAL_TOKENS:
                        field_violations.append(f'total({total_record_tokens})')

                    if field_violations:
                        validation_results['token_stats']['records_exceeding_limit'] += 1
                        if len(exceeding_records) < 5:
                            exceeding_records.append({
                                'line_number': line_count,
                                'tokens': {
                                    'system': sys_tokens,
                                    'user': usr_tokens,
                                    'assistant': ast_tokens,
                                    'total': total_record_tokens
                                },
                                'violations': field_violations
                            })

            except (json.JSONDecodeError, TypeError):
                invalid_json += 1

    file_size_mb = jsonl_path.stat().st_size / 1024 / 1024
    avg_tokens = total_tokens / line_count if line_count > 0 else 0

    validation_results['checks'].append({
        'name': 'integrity_check',
        'records': line_count,
        'invalid_json': invalid_json,
        'missing_fields': missing_fields_count,
        'size_mb': round(file_size_mb, 2)
    })
    validation_results['kategori_distribution'] = kategori_counts

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
        else:
            validation_results['checks'].append({
                'name': 'token_limit_check',
                'status': 'PASS',
                'message': 'All records within token limits'
            })

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
    
    # Kategori di luar ekspektasi (Unknown dll)
    other_kats = {k: v for k, v in kategori_counts.items() if k not in expected_categories}
    for kat, count in other_kats.items():
        pct = (count / line_count * 100) if line_count > 0 else 0
        bar = "▒" * int(pct / 2)
        print(f"   {kat:20s} | {count:>10,} | {pct:>6.2f}% | {bar}")

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

    report_path = Path('data/processed/validation_report.json')
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(validation_results, f, indent=2)

    if validation_results['status'] == 'PASS':
        print("\n" + "=" * 80)
        print("✅ ALL VALIDATION CHECKS PASSED! Data is ready for Model Training.")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("⚠️  VALIDATION FAILED! Check the report for details.")
        print("=" * 80)

    return validation_results

if __name__ == "__main__":
    run_validation('data/processed/training_data_balanced.jsonl', check_tokens=True)