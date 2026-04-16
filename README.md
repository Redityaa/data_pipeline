# 🗂️ Pipeline ETL: Data Tabular Sosio-Ekonomi → ChatML/JSONL

Pipeline pemrosesan data (ETL) yang mengubah data tabular **DTSEN (Data Terpadu Sosial Ekonomi Nasional)** menjadi format teks naratif berskema **ChatML/JSONL**, siap digunakan untuk *fine-tuning* atau *prompting* Large Language Model (LLM).

---

## 📋 Daftar Isi

- [Gambaran Umum](#gambaran-umum)
- [Struktur Proyek](#struktur-proyek)
- [Persyaratan Sistem](#persyaratan-sistem)
- [Instalasi](#instalasi)
- [Cara Penggunaan](#cara-penggunaan)
- [Arsitektur Pipeline](#arsitektur-pipeline)
- [Format Output](#format-output)
- [Konfigurasi](#konfigurasi)
- [Validasi Output](#validasi-output)
- [Hasil Benchmark](#hasil-benchmark)

---

## Gambaran Umum

Dataset DTSEN mengandung profil sosio-ekonomi keluarga yang dikodekan secara numerik (standar BPS). Pipeline ini melakukan tiga fase transformasi:

| Fase | Nama | Deskripsi |
|:---:|---|---|
| **A** | Cleaning & Reduksi | Drop kode wilayah, anonimisasi PII, handle missing values |
| **B** | Tekstualisasi | Konversi kode numerik → paragraf narasi Bahasa Indonesia |
| **C** | ChatML Packaging | Kemas narasi menjadi struktur `system / user / assistant` |

### Masalah yang Diselesaikan

- **Redundansi geografis** — Kolom `kode_provinsi`, `kode_kabupaten_kota`, dst. dibuang; hanya nama wilayah yang dipertahankan.
- **Privasi & PII** — `nomor_kartu_keluarga` di-hash, `nama_anggota_keluarga` di-redact, `id_pelanggan_pln` di-mask, dan `alamat` dihapus.
- **Kode biner mentah** — Nilai seperti `aset_bergerak_tv_datar = 1` diubah menjadi kalimat natural: *"Aset bergerak yang dimiliki: televisi datar."*

---

## Struktur Proyek

```
data_pipeline/
│
├── data/
│   ├── raw/
│   │   ├── dataset_dtsen_malang.csv            # Dataset input
│   │   └── metadata-dataset-dtsen-malang.pdf   # Buku kode BPS (codebook)
│   └── processed/
│       └── output.jsonl                        # ← Output pipeline
│
├── src/
│   ├── __init__.py
│   ├── pipeline.py          # Entry point utama
│   ├── cleaner.py           # Fase A: Cleaning & Anonimisasi PII
│   ├── tekstualisasi.py     # Fase B: Data-to-Text Generation
│   ├── chatml.py            # Fase C: ChatML Formatter
│   └── code_mappings.py     # Mapping kode BPS → label teks Indonesia
│
├── logs/
│   ├── pipeline.log         # Log eksekusi
│   └── missing_report.txt   # Laporan missing values per kolom
│
├── config.yaml              # Konfigurasi pipeline (path, chunk, prompt)
├── validate_output.py       # Validator JSONL output
├── requirements.txt
└── README.md
```

---

## Persyaratan Sistem

- Python **≥ 3.10**
- RAM: minimal **2 GB** (pipeline berjalan chunked, aman untuk 200K+ baris)

---

## Instalasi

```bash
# 1. Clone atau masuk ke direktori proyek
cd data_pipeline

# 2. (Opsional) Buat virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/macOS

# 3. Install dependensi
pip install -r requirements.txt
```

---

## Cara Penggunaan

### Menjalankan Pipeline

```bash
python src/pipeline.py
```

Pipeline akan membaca konfigurasi dari `config.yaml` secara otomatis. Untuk menentukan file konfigurasi secara eksplisit:

```bash
python src/pipeline.py --config config.yaml
```

Progress bar akan tampil di terminal selama proses berlangsung:

```
Menghitung total baris CSV …
Total baris data: 200,000 (0.1s)
Memproses: 100%|██████████| 200000/200000 [00:19<00:00, 5200.00baris/s]
```

### Memvalidasi Output

```bash
# Validasi dasar dengan 5 sampel acak
python validate_output.py

# Tampilkan 10 sampel acak
python validate_output.py --samples 10

# Ubah batas peringatan token (default 500)
python validate_output.py --token-limit 400

# Validasi file output di lokasi lain
python validate_output.py --file path/ke/output.jsonl
```

---

## Arsitektur Pipeline

```
dataset_dtsen_malang.csv
         │
         ▼  (chunk 10.000 baris)
┌─────────────────────────────┐
│  FASE A — cleaner.py        │
│  • Drop 5 kolom kode/alamat │
│  • Hash NKK (SHA-256)       │
│  • Mask ID PLN              │
│  • Redact nama KK           │
│  • Fill missing values      │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│  FASE B — tekstualisasi.py  │
│  • Blok 1: Demografi & PBI  │
│  • Blok 2: Perumahan        │
│  • Blok 3: Aset & Ternak    │
│  • Blok 4: Skor (assistant) │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│  FASE C — chatml.py         │
│  • Bungkus ke messages[]    │
│  • system / user / asst     │
│  • Tulis ke .jsonl          │
└────────────┬────────────────┘
             │
             ▼
      output.jsonl
```

### Mapping Kode BPS (code_mappings.py)

Semua kode integer BPS/DTSEN dipetakan ke label teks berdasarkan buku kode resmi (`metadata-dataset-dtsen-malang.pdf`):

| Variabel | Contoh Kode → Label |
|---|---|
| `status_kepemilikan_rumah` | `1` → "milik sendiri", `2` → "kontrak/sewa" |
| `jenis_lantai_terluas` | `1` → "marmer/granit", `6` → "semen/bata merah", `8` → "tanah" |
| `jenis_dinding_terluas` | `1` → "tembok", `4` → "anyaman bambu" |
| `jenis_atap_terluas` | `1` → "beton", `2` → "genteng", `3` → "seng" |
| `sumber_air_minum_utama` | `1` → "air kemasan", `4` → "sumur bor/pompa", `10` → "air hujan" |
| `sumber_penerangan_utama` | `1` → "listrik PLN dengan meteran", `4` → "bukan listrik" |
| `daya_terpasang` | `1` → "450 watt", `2` → "900 watt", `3` → "1.300 watt" |
| `bahan_bakar_utama_memasak` | `0` → "tidak memasak", `4` → "gas elpiji 3 kg", `10` → "kayu bakar" |
| `fasilitas_bab` | `1` → "ada, digunakan sendiri", `6` → "tidak ada fasilitas" |
| `jenis_kloset` | `1` → "leher angsa", `4` → "cemplung/cubluk" |
| `pembuangan_akhir_tinja` | `1` → "tangki septik", `3` → "kolam/sawah/sungai" |

---

## Format Output

Setiap baris `output.jsonl` adalah satu record JSON dengan skema ChatML:

```json
{
  "messages": [
    {
      "role": "system", 
      "content": "Anda adalah sistem ahli analisis profil kesejahteraan sosial yang objektif dan presisi. \nTugas Anda: 1. Menganalisis kondisi sosial-ekonomi keluarga berdasarkan deskripsi yang diberikan. 2. Memberikan penalaran (reasoning) komprehensif yang mencakup aspek: - Kepemilikan aset dan kualitas hunian. - Komposisi anggota keluarga dan beban ketergantungan. - Stabilitas pendapatan dan akses kebutuhan dasar.\n3. Menentukan skor evaluasi internal (0-100) dan estimasi desil nasional (1-10) berdasarkan kriteria kemiskinan makro yang berlaku.\nGunakan format output berikut: - Analisis Kondisi: (Bedah poin-poin krusial dari deskripsi) - Reasoning: (Penjelasan mengapa keluarga tersebut masuk ke kategori skor/desil tertentu, hubungkan antar variabel) - Skor Evaluasi: [Angka] - Desil Nasional: [Angka 1-10]"
    },
    {
      "role": "user", 
      "content": "Profil Keluarga:\n\n[Demografi & Lokasi]\nKeluarga ini berlokasi di Kelurahan Arjowinangun, Kecamatan Kedungkandang, Kota Malang, Provinsi Jawa Timur. Keluarga terdiri dari 6 orang anggota. Keluarga ini tercatat sebagai penerima Bantuan Iuran Jaminan Kesehatan Nasional (PBI-JKN) dan penerima Bantuan Iuran Pemda.\n\n[Kondisi Perumahan]\nMereka menempati rumah berstatus lainnya dengan luas lantai 20 meter persegi. Jenis lantai: semen/bata merah; dinding: anyaman bambu; atap: lainnya. Sumber air minum utama dari mata air tak terlindung. Penerangan utama bukan listrik (bukan listrik (lampu minyak/lilin/obor)). Bahan bakar utama untuk memasak adalah briket. Fasilitas BAB: tidak ada fasilitas BAB, dengan jenis kloset cemplung/cubluk dan pembuangan akhir tinja ke pantai/tanah lapang/kebun.\n\n[Kepemilikan Aset & Ternak]\nTidak memiliki aset bergerak yang tercatat. Tidak memiliki lahan atau rumah lain selain yang dihuni. Tidak memiliki hewan ternak."
    },
    {
      "role": "assistant", 
      "content": "Analisis Kondisi:\n- Demografi & Lokasi: Keluarga ini berlokasi di Kelurahan Arjowinangun, Kecamatan Kedungkandang, Kota Malang, Provinsi Jawa Timur. Keluarga terdiri dari 6 orang anggota. Keluarga ini tercatat sebagai penerima Bantuan Iuran Jaminan Kesehatan Nasional (PBI-JKN) dan penerima Bantuan Iuran Pemda.\n- Kondisi Perumahan: Mereka menempati rumah berstatus lainnya dengan luas lantai 20 meter persegi. Jenis lantai: semen/bata merah; dinding: anyaman bambu; atap: lainnya. Sumber air minum utama dari mata air tak terlindung. Penerangan utama bukan listrik (bukan listrik (lampu minyak/lilin/obor)). Bahan bakar utama untuk memasak adalah briket. Fasilitas BAB: tidak ada fasilitas BAB, dengan jenis kloset cemplung/cubluk dan pembuangan akhir tinja ke pantai/tanah lapang/kebun.\n- Kepemilikan Aset & Ternak: Tidak memiliki aset bergerak yang tercatat. Tidak memiliki lahan atau rumah lain selain yang dihuni. Tidak memiliki hewan ternak.\n\nReasoning: Jumlah anggota keluarga yang besar (6 orang) mengindikasikan beban ketergantungan tinggi yang menekan kapasitas ekonomi rumah tangga. Tercatatnya keluarga sebagai PBI-JKN nasional dan PBI Pemda menunjukkan pengakuan formal sebagai kelompok rentan/miskin. Minimnya kepemilikan aset bergerak maupun tidak bergerak mencerminkan keterbatasan akumulasi kekayaan dan ketahanan ekonomi yang rendah.\n\nSkor Evaluasi: 0.00\nDesil Nasional: 1"
    }
  ]
}
```

---

## Konfigurasi

Semua parameter pipeline diatur di `config.yaml`:

```yaml
paths:
  input_csv:   "data/raw/dataset_dtsen_malang.csv"
  output_file: "data/processed/output.jsonl"
  log_dir:     "logs"

processing:
  chunk_size:   10000      # Baris per iterasi (turunkan jika RAM terbatas)
  csv_encoding: "utf-8"

chatml:
  system_prompt: >
    Anda adalah sistem ahli analisis profil kesejahteraan sosial yang objektif dan presisi. 
    
    Tugas Anda:
    1. Menganalisis kondisi sosial-ekonomi keluarga berdasarkan deskripsi yang diberikan.
    2. Memberikan penalaran (reasoning) komprehensif yang mencakup aspek:
    - Kepemilikan aset dan kualitas hunian.
    - Komposisi anggota keluarga dan beban ketergantungan.
    - Stabilitas pendapatan dan akses kebutuhan dasar.

    3. Menentukan skor evaluasi internal (0-100) dan estimasi desil nasional (1-10) berdasarkan kriteria kemiskinan makro yang berlaku.

    Gunakan format output berikut:
    - Analisis Kondisi: (Bedah poin-poin krusial dari deskripsi)
    - Reasoning: (Penjelasan mengapa keluarga tersebut masuk ke kategori skor/desil tertentu, hubungkan antar variabel)
    - Skor Evaluasi: [Angka]
    - Desil Nasional: [Angka 1-10]
```

---

## Validasi Output

Validator (`validate_output.py`) melakukan pengecekan berikut secara otomatis:

| Cek | Metode |
|---|---|
| JSON valid per baris | `json.loads()` |
| Skema 3 messages (system/user/assistant) | Cek panjang & role |
| Content tidak kosong | String check |
| Tidak ada kode wilayah bocor | Regex `\d{2}\.\d{2}` |
| Tidak ada NKK mentah (16 digit) | Regex `\d{16}` |
| Tidak ada ID PLN mentah (8+ digit) | Regex + konteks prefix `PLN-` |
| Estimasi token per record | Heuristik `len(text) / 4` |

---

## Hasil Benchmark

Diuji pada dataset DTSEN Kota Malang (200.000 baris input):

| Metrik | Nilai |
|---|---|
| Total record output | **100.000** |
| Ukuran file output | **138.7 MB** |
| Waktu eksekusi | **~19 detik** |
| Error struktural | **0** |
| Rata-rata token/record | **289 token** |
| Maksimum token/record | **344 token** |
| Record melebihi 500 token | **0 (0%)** |
| Kebocoran kode wilayah/PII | **0** |
| Missing values | **0** |
