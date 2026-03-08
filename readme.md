# Data Preparation Pipeline
Pipeline otomatis untuk mempersiapkan dataset kesejahteraan Jawa Timur agar siap digunakan untuk pelatihan model machine learning/LLM.
## 📋 Deskripsi
Repo ini berisi pipeline data preparation yang melakukan:
- **ETL (Extraction, Transformation, Loading)**: Membersihkan dan memvalidasi data mentah
- **Rebalancing Dataset**: Menyeimbangkan distribusi kelas dengan teknik undersampling
- **Validasi Output**: Memastikan kualitas data sebelum digunakan untuk modeling
Pipeline ini dirancang untuk menangani dataset besar (jutaan record) dengan pemrosesan bertahap (chunking) dan logging yang komprehensif.
## 🏗️ Struktur Direktori
```
├── configs/                 # File konfigurasi pipeline
│   ├── data_schema.yaml     # Schema validasi data
│   ├── labeling_config.yaml # Konfigurasi labeling
│   ├── pipeline_config.yaml # Pengaturan pipeline utama
│   └── security_config.yaml # Konfigurasi keamanan
├── src/                     # Source code modular
│   ├── ingestion/          # Modul ingest data
│   ├── labeling/           # Modul labeling
│   ├── security/           # Modul keamanan & enkripsi
│   ├── transformation/     # ETL & rebalancing
│   │   ├── etl_pipeline.py
│   │   └── rebalance_dataset.py
│   └── validation/         # Validasi output
│       ├── validate_output.py
│       ├── schema_validator.py
│       └── quality_checker.py
├── data/                    # Direktori data (buat manual)
│   ├── raw/                # Data mentah input
│   ├── processed/          # Data hasil processing
│   └── logs/               # Log pipeline
├── .env                     # Environment variables
├── .gitignore              # Git ignore rules
├── requirements.txt        # Dependencies Python
└── run_pipeline.py         # Entry point utama
```
## ⚙️ Prasyarat
- Python 3.8+
- pip (Python package manager)
## 🚀 Instalasi
1. **Clone repository ini**
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```
2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
3. **Setup environment variables**
   Edit file `.env` dan isi `DATA_SALT` dengan string random untuk keamanan data:
   ```bash
   DATA_SALT="your_random_secure_string_here"
   ```
   > ⚠️ **Penting**: Ganti `paste_your_random_string_here` dengan string random Anda sendiri sebelum menjalankan pipeline di production.
4. **Siapkan struktur direktori data**
   ```bash
   mkdir -p data/raw data/processed data/logs
   ```
5. **Tempatkan data mentah**
   Letakkan file CSV Anda di `data/raw/dataset_kesejahteraan_jatim.csv`
## 📖 Cara Penggunaan
### Menjalankan Pipeline Lengkap
```bash
python run_pipeline.py
```
Pipeline akan menjalankan 3 fase secara berurutan:
#### Fase 1: ETL (Extraction, Transformation, Loading)
- Membaca data dari `data/raw/dataset_kesejahteraan_jatim.csv`
- Melakukan cleaning dan transformasi
- Menyimpan hasil ke `data/processed/training_data.jsonl`
#### Fase 2: Rebalancing Dataset
- Melakukan undersampling untuk menyeimbangkan distribusi kelas
- Target: 100.000 record per kelas
- Output: `data/processed/training_data_balanced.jsonl`
#### Fase 3: Validasi Output
- Memvalidasi schema dan kualitas data
- Memastikan data siap untuk modeling
### Logging
Semua log tersimpan di:
- **File**: `data/logs/pipeline.log`
- **Console**: Output real-time di terminal
### Konfigurasi
Edit file di folder `configs/` untuk menyesuaikan:
- **pipeline_config.yaml**: Chunk size, threshold error, format output
- **data_schema.yaml**: Schema validasi data
- **labeling_config.yaml**: Aturan labeling
- **security_config.yaml**: Pengaturan keamanan
## 📦 Output
Setelah pipeline berhasil, Anda akan mendapatkan:
| File | Deskripsi |
|------|-----------|
| `data/processed/training_data.jsonl` | Data hasil ETL |
| `data/processed/training_data_balanced.jsonl` | Data balanced siap training |
| `data/logs/pipeline.log` | Log lengkap eksekusi |
Format output: **JSONL** (JSON Lines) - optimal untuk pelatihan LLM.
## 🔍 Troubleshooting
### Error: DATA_SALT not set
```bash
# Edit file .env dan isi DATA_SALT dengan string random
nano .env
```
### Error: File tidak ditemukan
```bash
# Pastikan file input ada di lokasi yang benar
ls data/raw/dataset_kesejahteraan_jatim.csv
```
### Error: Dependencies missing
```bash
# Install ulang dependencies
pip install -r requirements.txt --upgrade
```