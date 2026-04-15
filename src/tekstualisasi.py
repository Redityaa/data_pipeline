"""
tekstualisasi.py
================
Fase B: Data-to-Text Generation

Mengubah setiap baris DataFrame menjadi empat blok narasi:
  - blok_demografi  : lokasi + jumlah AK + status PBI
  - blok_perumahan  : kondisi fisik rumah, air, sanitasi, energi
  - blok_aset       : aset bergerak, tidak bergerak, ternak
  - blok_skor       : skor evaluasi + desil nasional (untuk role assistant)
"""

import logging
from typing import Any, Tuple

import pandas as pd

from src.code_mappings import (
    BAHAN_BAKAR_MEMASAK,
    DAYA_TERPASANG,
    FASILITAS_BAB,
    JENIS_ATAP,
    JENIS_DINDING,
    JENIS_KLOSET,
    JENIS_LANTAI,
    LABEL_ASET_BERGERAK,
    LABEL_ASET_TIDAK_BERGERAK,
    LABEL_TERNAK,
    PEMBUANGAN_TINJA,
    STATUS_KEPEMILIKAN_RUMAH,
    SUMBER_AIR_MINUM,
    SUMBER_PENERANGAN,
    lookup,
)

logger = logging.getLogger(__name__)

# Nilai sentinel dari cleaner.py (numeric fill)
_SENTINEL = -1


def _safe(val: Any, default: str = "tidak diketahui") -> str:
    """Kembalikan string aman; handle sentinel & NaN."""
    if pd.isna(val) if isinstance(val, float) else False:
        return default
    if val == _SENTINEL or val == -1:
        return default
    return str(val).strip() or default


def _title(s: str) -> str:
    """Title-case untuk nama wilayah."""
    return s.strip().title() if s and s != "Tidak Diketahui" else s


# ---------------------------------------------------------------------------
# Blok 1 — Demografi & Lokasi
# ---------------------------------------------------------------------------
def _blok_demografi(row: pd.Series) -> str:
    kelurahan  = _title(_safe(row.get("kelurahan_desa", ""), "suatu kelurahan"))
    kecamatan  = _title(_safe(row.get("kecamatan", ""), "suatu kecamatan"))
    kabupaten  = _title(_safe(row.get("kabupaten_kota", ""), "suatu kabupaten/kota"))
    provinsi   = _title(_safe(row.get("provinsi", ""), "suatu provinsi"))

    jml_ak = row.get("jumlah_anggota_keluarga", _SENTINEL)
    if jml_ak == _SENTINEL or pd.isna(jml_ak):
        jml_ak_str = "sejumlah yang tidak diketahui"
    else:
        jml_ak_int = int(jml_ak)
        jml_ak_str = f"{jml_ak_int} orang"

    # Status PBI
    pbi_nas   = str(row.get("pbi_nas",  "")).strip().upper()
    pbi_pemda = str(row.get("pbi_pemda", "")).strip().upper()
    pbi_parts = []
    if pbi_nas == "YA":
        pbi_parts.append("penerima Bantuan Iuran Jaminan Kesehatan Nasional (PBI-JKN)")
    if pbi_pemda == "YA":
        pbi_parts.append("penerima Bantuan Iuran Pemda")

    if pbi_parts:
        pbi_str = "Keluarga ini tercatat sebagai " + " dan ".join(pbi_parts) + "."
    else:
        pbi_str = "Keluarga ini tidak tercatat sebagai penerima bantuan iuran (non-PBI)."

    return (
        f"Keluarga ini berlokasi di Kelurahan {kelurahan}, Kecamatan {kecamatan}, "
        f"{kabupaten}, Provinsi {provinsi}. "
        f"Keluarga terdiri dari {jml_ak_str} anggota. "
        f"{pbi_str}"
    )


# ---------------------------------------------------------------------------
# Blok 2 — Perumahan
# ---------------------------------------------------------------------------
def _blok_perumahan(row: pd.Series) -> str:
    kepemilikan = lookup(STATUS_KEPEMILIKAN_RUMAH, row.get("status_kepemilikan_rumah"))
    lantai      = lookup(JENIS_LANTAI,             row.get("jenis_lantai_terluas"))
    dinding     = lookup(JENIS_DINDING,            row.get("jenis_dinding_terluas"))
    atap        = lookup(JENIS_ATAP,               row.get("jenis_atap_terluas"))
    air         = lookup(SUMBER_AIR_MINUM,         row.get("sumber_air_minum_utama"))
    penerangan  = lookup(SUMBER_PENERANGAN,        row.get("sumber_penerangan_utama"))
    daya        = lookup(DAYA_TERPASANG,           row.get("daya_terpasang"))
    bahan_bakar = lookup(BAHAN_BAKAR_MEMASAK,      row.get("bahan_bakar_utama_memasak"))
    fasilitas   = lookup(FASILITAS_BAB,            row.get("fasilitas_bab"))
    kloset      = lookup(JENIS_KLOSET,             row.get("jenis_kloset"))
    tinja       = lookup(PEMBUANGAN_TINJA,         row.get("pembuangan_akhir_tinja"))

    luas_raw = row.get("luas_lantai", _SENTINEL)
    luas_str = (
        f"{int(luas_raw)} meter persegi"
        if luas_raw not in (_SENTINEL, None) and not pd.isna(luas_raw) and int(luas_raw) > 0
        else "luas tidak tercatat"
    )

    # Sentence khusus bahan bakar
    if row.get("bahan_bakar_utama_memasak", _SENTINEL) == 0:
        bahan_bakar_str = "Keluarga ini tidak memasak di rumah."
    else:
        bahan_bakar_str = f"Bahan bakar utama untuk memasak adalah {bahan_bakar}."

    # Sentence khusus penerangan + daya
    penerangan_val = row.get("sumber_penerangan_utama", _SENTINEL)
    if penerangan_val in (1, 2):  # PLN
        penerangan_str = f"Penerangan utama menggunakan {penerangan} dengan daya terpasang {daya}."
    elif penerangan_val == 3:     # Non-PLN
        penerangan_str = f"Penerangan utama menggunakan {penerangan}."
    else:
        penerangan_str = f"Penerangan utama bukan listrik ({penerangan})."

    return (
        f"Mereka menempati rumah berstatus {kepemilikan} dengan luas lantai {luas_str}. "
        f"Jenis lantai: {lantai}; dinding: {dinding}; atap: {atap}. "
        f"Sumber air minum utama dari {air}. "
        f"{penerangan_str} "
        f"{bahan_bakar_str} "
        f"Fasilitas BAB: {fasilitas}, dengan jenis kloset {kloset} "
        f"dan pembuangan akhir tinja ke {tinja}."
    )


# ---------------------------------------------------------------------------
# Blok 3 — Aset & Ternak
# ---------------------------------------------------------------------------
def _blok_aset(row: pd.Series) -> str:
    # Aset bergerak (nilai 1 = YA)
    aset_dimiliki = [
        label
        for col, label in LABEL_ASET_BERGERAK.items()
        if row.get(col, 2) == 1
    ]

    # Aset tidak bergerak
    aset_tetap = [
        label
        for col, label in LABEL_ASET_TIDAK_BERGERAK.items()
        if row.get(col, 2) == 1
    ]

    # Ternak
    ternak_list = []
    for col, label in LABEL_TERNAK.items():
        val = row.get(col, 0)
        try:
            n = int(val)
        except (ValueError, TypeError):
            n = 0
        if n > 0:
            ternak_list.append(f"{n} ekor {label}")

    # Rangkai narasi aset bergerak
    if aset_dimiliki:
        aset_str = "Aset bergerak yang dimiliki: " + ", ".join(aset_dimiliki) + "."
    else:
        aset_str = "Tidak memiliki aset bergerak yang tercatat."

    # Narasi aset tidak bergerak
    if aset_tetap:
        tetap_str = "Memiliki aset tidak bergerak berupa: " + ", ".join(aset_tetap) + "."
    else:
        tetap_str = "Tidak memiliki lahan atau rumah lain selain yang dihuni."

    # Narasi ternak
    if ternak_list:
        ternak_str = "Hewan ternak yang dimiliki: " + ", ".join(ternak_list) + "."
    else:
        ternak_str = "Tidak memiliki hewan ternak."

    return f"{aset_str} {tetap_str} {ternak_str}"


# ---------------------------------------------------------------------------
# Blok 4 — Assistant Content (sesuai format system prompt)
# ---------------------------------------------------------------------------
def _generate_assistant_content(
    row: pd.Series,
    blok_demografi: str,
    blok_perumahan: str,
    blok_aset: str,
) -> str:
    """
    Hasilkan respons terstruktur sesuai format output yang ditentukan system prompt:
      - Analisis Kondisi
      - Reasoning
      - Skor Evaluasi
      - Desil Nasional
    """
    skor_raw  = row.get("skor", None)
    desil_raw = row.get("desil_nasional", None)

    try:
        skor  = float(skor_raw) if skor_raw not in (None, _SENTINEL, "") else None
    except (ValueError, TypeError):
        skor = None

    try:
        desil = int(desil_raw) if desil_raw not in (None, _SENTINEL, "") else None
    except (ValueError, TypeError):
        desil = None

    skor_str  = f"{skor:.2f}" if skor  is not None else "tidak tersedia"
    desil_str = str(desil)    if desil is not None else "tidak tersedia"

    # --- Analisis Kondisi: poin-poin krusial dari tiap blok ---
    analisis_kondisi = (
        f"- Demografi & Lokasi: {blok_demografi}\n"
        f"- Kondisi Perumahan: {blok_perumahan}\n"
        f"- Kepemilikan Aset & Ternak: {blok_aset}"
    )

    # --- Reasoning: hubungkan variabel antar blok menuju skor/desil ---
    # Ambil sinyal kunci untuk reasoning
    jml_ak    = row.get("jumlah_anggota_keluarga", _SENTINEL)
    jml_ak_i  = int(jml_ak) if jml_ak not in (_SENTINEL, None) and not pd.isna(jml_ak) else None

    pbi_parts = []
    if str(row.get("pbi_nas",  "")).strip().upper() == "YA":
        pbi_parts.append("PBI-JKN nasional")
    if str(row.get("pbi_pemda", "")).strip().upper() == "YA":
        pbi_parts.append("PBI Pemda")

    # Aset bergerak
    from src.code_mappings import LABEL_ASET_BERGERAK, LABEL_ASET_TIDAK_BERGERAK
    aset_bergerak_count = sum(
        1 for col in LABEL_ASET_BERGERAK if row.get(col, 2) == 1
    )
    aset_tetap_count = sum(
        1 for col in LABEL_ASET_TIDAK_BERGERAK if row.get(col, 2) == 1
    )

    reasoning_parts = []

    # Beban ketergantungan
    if jml_ak_i is not None:
        if jml_ak_i >= 6:
            reasoning_parts.append(
                f"Jumlah anggota keluarga yang besar ({jml_ak_i} orang) mengindikasikan "
                "beban ketergantungan tinggi yang menekan kapasitas ekonomi rumah tangga."
            )
        elif jml_ak_i <= 2:
            reasoning_parts.append(
                f"Keluarga kecil ({jml_ak_i} orang) memiliki beban ketergantungan rendah "
                "yang mendukung efisiensi pengeluaran."
            )
        else:
            reasoning_parts.append(
                f"Komposisi keluarga ({jml_ak_i} orang) tergolong sedang "
                "dengan beban ketergantungan yang moderat."
            )

    # Status PBI
    if pbi_parts:
        reasoning_parts.append(
            "Tercatatnya keluarga sebagai "
            + " dan ".join(pbi_parts)
            + " menunjukkan pengakuan formal sebagai kelompok rentan/miskin."
        )
    else:
        reasoning_parts.append(
            "Status non-PBI menunjukkan keluarga belum atau tidak teridentifikasi "
            "dalam program bantuan iuran kesehatan resmi."
        )

    # Aset
    if aset_bergerak_count == 0 and aset_tetap_count == 0:
        reasoning_parts.append(
            "Minimnya kepemilikan aset bergerak maupun tidak bergerak "
            "mencerminkan keterbatasan akumulasi kekayaan dan ketahanan ekonomi yang rendah."
        )
    elif aset_bergerak_count + aset_tetap_count >= 4:
        reasoning_parts.append(
            f"Kepemilikan aset yang relatif beragam ({aset_bergerak_count} aset bergerak, "
            f"{aset_tetap_count} aset tidak bergerak) berkontribusi positif terhadap skor kesejahteraan."
        )
    else:
        reasoning_parts.append(
            f"Kepemilikan aset terbatas ({aset_bergerak_count} aset bergerak, "
            f"{aset_tetap_count} aset tidak bergerak) menunjukkan akumulasi kekayaan yang masih rendah."
        )

    reasoning = " ".join(reasoning_parts)

    return (
        f"Analisis Kondisi:\n{analisis_kondisi}\n\n"
        f"Reasoning: {reasoning}\n\n"
        f"Skor Evaluasi: {skor_str}\n"
        f"Desil Nasional: {desil_str}"
    )


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def tekstualisasi_row(row: pd.Series) -> Tuple[str, str]:
    """
    Konversikan satu baris menjadi pasangan (user_content, assistant_content).

    assistant_content mengikuti format output dari system prompt:
      - Analisis Kondisi
      - Reasoning
      - Skor Evaluasi
      - Desil Nasional

    Returns
    -------
    (user_content, assistant_content)
    """
    blok1 = _blok_demografi(row)
    blok2 = _blok_perumahan(row)
    blok3 = _blok_aset(row)

    user_content = (
        "Profil Keluarga:\n\n"
        f"[Demografi & Lokasi]\n{blok1}\n\n"
        f"[Kondisi Perumahan]\n{blok2}\n\n"
        f"[Kepemilikan Aset & Ternak]\n{blok3}"
    )

    assistant_content = _generate_assistant_content(row, blok1, blok2, blok3)

    return user_content, assistant_content


def tekstualisasi_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Terapkan tekstualisasi ke seluruh chunk.

    Returns DataFrame dengan dua kolom tambahan:
      - user_content
      - assistant_content
    """
    results = [tekstualisasi_row(row) for _, row in df.iterrows()]
    df = df.copy()
    df["user_content"]      = [r[0] for r in results]
    df["assistant_content"] = [r[1] for r in results]
    return df
