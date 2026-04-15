"""
validate_output.py
==================
Validator JSONL output dari pipeline ETL DTSEN.

Cek yang dilakukan:
  1. Setiap baris adalah JSON valid
  2. Skema messages: tepat 3 item (system, user, assistant)
  3. Tidak ada kode wilayah BPS yang bocor dalam narasi (format XX.XX.XX)
  4. Tidak ada NKK (16 digit) atau ID PLN asli
  5. Estimasi token per record (heuristik: len/4) — peringatan jika > 500
  6. Laporan ringkas di akhir

Jalankan:
  python validate_output.py
  python validate_output.py --file data/processed/output.jsonl --samples 10
"""

import argparse
import json
import random
import re
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Pola regex berbahaya (bocoran kode administrasi / PII mentah)
# ---------------------------------------------------------------------------
# Kode BPS format: 35.73.03.1011 atau 35.73 dll.
RE_KODE_WILAYAH = re.compile(r"\b\d{2}\.\d{2}(?:\.\d{2,5})?\b")

# NKK: 16 digit berurutan
RE_NKK = re.compile(r"\b\d{16}\b")

# ID PLN mentah: 8+ digit berurutan (sebelum masking)
RE_PLN_RAW = re.compile(r"\b\d{8,}\b")


# ---------------------------------------------------------------------------
# Fungsi Validasi
# ---------------------------------------------------------------------------
def validate_line(line: str, line_num: int) -> list[str]:
    """
    Validasi satu baris JSONL.

    Returns
    -------
    List error string (kosong jika valid).
    """
    errors = []

    # 1. JSON valid?
    try:
        record = json.loads(line)
    except json.JSONDecodeError as e:
        return [f"Baris {line_num}: JSON tidak valid — {e}"]

    # 2. Skema messages
    msgs = record.get("messages", None)
    if not isinstance(msgs, list):
        errors.append(f"Baris {line_num}: 'messages' bukan list")
        return errors

    if len(msgs) != 3:
        errors.append(
            f"Baris {line_num}: Jumlah messages = {len(msgs)} (harusnya 3)"
        )

    expected_roles = ["system", "user", "assistant"]
    for i, (msg, exp_role) in enumerate(zip(msgs, expected_roles)):
        if not isinstance(msg, dict):
            errors.append(f"Baris {line_num}: messages[{i}] bukan dict")
            continue
        role = msg.get("role", "")
        if role != exp_role:
            errors.append(
                f"Baris {line_num}: messages[{i}].role = '{role}' "
                f"(harusnya '{exp_role}')"
            )
        if not msg.get("content", "").strip():
            errors.append(f"Baris {line_num}: messages[{i}].content kosong")

    # 3 & 4. Cek kebocoran dalam teks narasi (user + assistant)
    all_text = " ".join(
        msg.get("content", "") for msg in msgs if isinstance(msg, dict)
    )

    if RE_KODE_WILAYAH.search(all_text):
        match = RE_KODE_WILAYAH.search(all_text)
        errors.append(
            f"Baris {line_num}: BOCOR kode wilayah → '{match.group()}'"
        )

    if RE_NKK.search(all_text):
        errors.append(f"Baris {line_num}: BOCOR NKK (16 digit) terdeteksi")

    # PLN mentah: hanya jika tidak mengandung prefix PLN- yang sudah di-mask
    for m in RE_PLN_RAW.finditer(all_text):
        ctx_start = max(0, m.start() - 4)
        ctx = all_text[ctx_start:m.start()]
        if "PLN-" not in ctx:
            errors.append(
                f"Baris {line_num}: Potensi bocor ID PLN mentah → '{m.group()}'"
            )
            break  # satu peringatan per baris cukup

    return errors


def estimate_tokens(text: str) -> int:
    """Heuristik: 1 token ≈ 4 karakter."""
    return max(1, len(text) // 4)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Validator JSONL output pipeline DTSEN")
    parser.add_argument(
        "--file",
        default="data/processed/output.jsonl",
        help="Path ke file JSONL output (default: data/processed/output.jsonl)",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=5,
        help="Jumlah sampel acak yang dicetak ke stdout (default: 5)",
    )
    parser.add_argument(
        "--token-limit",
        type=int,
        default=500,
        help="Batas token per record untuk peringatan (default: 500)",
    )
    args = parser.parse_args()

    jsonl_path = Path(args.file)
    if not jsonl_path.exists():
        print(f"[ERROR] File tidak ditemukan: {jsonl_path}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"VALIDASI: {jsonl_path}")
    print(f"{'='*60}\n")

    total_lines    = 0
    error_count    = 0
    token_warnings = 0
    all_errors     = []
    sample_pool    = []
    token_stats    = []

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            total_lines += 1

            # Validasi struktural
            errs = validate_line(line, line_num)
            if errs:
                error_count += len(errs)
                all_errors.extend(errs[:3])  # simpan maks 3 error per baris

            # Estimasi token
            try:
                record = json.loads(line)
                user_content = next(
                    (m["content"] for m in record["messages"] if m["role"] == "user"), ""
                )
                asst_content = next(
                    (m["content"] for m in record["messages"] if m["role"] == "assistant"), ""
                )
                tok = estimate_tokens(user_content + asst_content)
                token_stats.append(tok)
                if tok > args.token_limit:
                    token_warnings += 1
            except Exception:
                pass

            # Simpan untuk sampling
            if len(sample_pool) < 1000:
                sample_pool.append(line)

    # ── Laporan ─────────────────────────────────────────────────────── #
    print(f"Total record      : {total_lines:,}")
    print(f"Error struktural  : {error_count:,}")

    if token_stats:
        avg_tok = sum(token_stats) / len(token_stats)
        max_tok = max(token_stats)
        min_tok = min(token_stats)
        over    = token_warnings
        print(f"\nEstimasi Token (heuristik len/4):")
        print(f"  Rata-rata : {avg_tok:.0f} token")
        print(f"  Minimum   : {min_tok} token")
        print(f"  Maksimum  : {max_tok} token")
        print(f"  > {args.token_limit} token : {over:,} record ({over/total_lines*100:.1f}%)")

    if all_errors:
        print(f"\n{'─'*60}")
        print(f"DAFTAR ERROR (maks 20 pertama):")
        for e in all_errors[:20]:
            print(f"  [WARN]  {e}")
    else:
        print("\n[OK]  Semua record valid - tidak ada error struktural atau kebocoran PII.")

    # ── Sampel acak ──────────────────────────────────────────────────── #
    if sample_pool and args.samples > 0:
        print(f"\n{'='*60}")
        n_show = min(args.samples, len(sample_pool))
        print(f"SAMPEL ACAK ({n_show} record):")
        samples = random.sample(sample_pool, n_show)
        for i, s in enumerate(samples, 1):
            record = json.loads(s)
            print(f"\n--- Sampel {i} ---")
            for msg in record["messages"]:
                role    = msg["role"].upper()
                content = msg["content"][:400]  # batasi panjang tampilan
                print(f"[{role}]\n{content}")
                if len(msg["content"]) > 400:
                    print("  … (dipotong)")

    # ── Exit code ────────────────────────────────────────────────────── #
    print(f"\n{'='*60}")
    if error_count == 0:
        print("[OK] Validasi LULUS")
        sys.exit(0)
    else:
        print(f"[FAIL] Validasi GAGAL - {error_count} error ditemukan")
        sys.exit(1)


if __name__ == "__main__":
    main()
