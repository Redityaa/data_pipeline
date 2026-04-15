"""
code_mappings.py
================
Mapping kode integer BPS/DTSEN → label teks Indonesia.
Sumber: metadata-dataset-dtsen-malang.pdf

Konvensi: semua nilai int, semua label str.
Nilai yang tidak ditemukan di mapping → "tidak diketahui".
"""

# ---------------------------------------------------------------------------
# STATUS KEPEMILIKAN RUMAH
# ---------------------------------------------------------------------------
STATUS_KEPEMILIKAN_RUMAH = {
    1: "milik sendiri",
    2: "kontrak/sewa",
    3: "bebas sewa",
    4: "dinas",
    5: "lainnya",
}

# ---------------------------------------------------------------------------
# JENIS LANTAI TERLUAS
# ---------------------------------------------------------------------------
JENIS_LANTAI = {
    1: "marmer/granit",
    2: "keramik",
    3: "parket/vinil/karpet",
    4: "ubin/tegel/teraso",
    5: "kayu/papan",
    6: "semen/bata merah",
    7: "bambu",
    8: "tanah",
    9: "lainnya",
}

# ---------------------------------------------------------------------------
# JENIS DINDING TERLUAS
# ---------------------------------------------------------------------------
JENIS_DINDING = {
    1: "tembok",
    2: "plesteran anyaman bambu/kawat",
    3: "kayu/papan/gypsum/GRC/calciboard",
    4: "anyaman bambu",
    5: "batang kayu",
    6: "bambu",
    7: "lainnya",
}

# ---------------------------------------------------------------------------
# JENIS ATAP TERLUAS
# ---------------------------------------------------------------------------
JENIS_ATAP = {
    1: "beton",
    2: "genteng",
    3: "seng",
    4: "asbes",
    5: "bambu",
    6: "kayu/sirap",
    7: "jerami/ijuk/daun-daunan/rumbia",
    8: "lainnya",
}

# ---------------------------------------------------------------------------
# SUMBER AIR MINUM UTAMA
# ---------------------------------------------------------------------------
SUMBER_AIR_MINUM = {
    1:  "air kemasan bermerk",
    2:  "air isi ulang",
    3:  "leding/PDAM",
    4:  "sumur bor/pompa",
    5:  "sumur terlindung",
    6:  "sumur tak terlindung",
    7:  "mata air terlindung",
    8:  "mata air tak terlindung",
    9:  "air permukaan (sungai/danau/waduk/kolam/irigasi)",
    10: "air hujan",
    11: "lainnya",
}

# ---------------------------------------------------------------------------
# SUMBER PENERANGAN UTAMA
# ---------------------------------------------------------------------------
SUMBER_PENERANGAN = {
    1: "listrik PLN dengan meteran",
    2: "listrik PLN tanpa meteran",
    3: "listrik non-PLN",
    4: "bukan listrik (lampu minyak/lilin/obor)",
}

# ---------------------------------------------------------------------------
# DAYA TERPASANG
# ---------------------------------------------------------------------------
DAYA_TERPASANG = {
    1: "450 watt",
    2: "900 watt",
    3: "1.300 watt",
    4: "2.200 watt",
    5: "lebih dari 2.200 watt",
}

# ---------------------------------------------------------------------------
# BAHAN BAKAR UTAMA MEMASAK
# ---------------------------------------------------------------------------
BAHAN_BAKAR_MEMASAK = {
    0:  "tidak memasak di rumah",
    1:  "listrik",
    2:  "gas elpiji 5,5 kg/blue gaz",
    3:  "gas elpiji 12 kg",
    4:  "gas elpiji 3 kg",
    5:  "gas kota/meteran PGN",
    6:  "biogas",
    7:  "minyak tanah",
    8:  "briket",
    9:  "arang",
    10: "kayu bakar",
    11: "lainnya",
}

# ---------------------------------------------------------------------------
# FASILITAS BAB
# ---------------------------------------------------------------------------
FASILITAS_BAB = {
    1: "ada, digunakan sendiri oleh anggota keluarga",
    2: "ada, digunakan bersama keluarga tertentu",
    3: "ada, di MCK komunal",
    4: "ada, di MCK umum/siapa pun dapat menggunakan",
    5: "ada, namun anggota keluarga tidak menggunakan",
    6: "tidak ada fasilitas BAB",
}

# ---------------------------------------------------------------------------
# JENIS KLOSET
# ---------------------------------------------------------------------------
JENIS_KLOSET = {
    1: "leher angsa",
    2: "plengsengan dengan tutup",
    3: "plengsengan tanpa tutup",
    4: "cemplung/cubluk",
}

# ---------------------------------------------------------------------------
# PEMBUANGAN AKHIR TINJA
# ---------------------------------------------------------------------------
PEMBUANGAN_TINJA = {
    1: "tangki septik",
    2: "IPAL (Instalasi Pengolahan Air Limbah)",
    3: "kolam/sawah/sungai/danau/laut",
    4: "lubang tanah",
    5: "pantai/tanah lapang/kebun",
    6: "lainnya",
}

# ---------------------------------------------------------------------------
# ASET BERGERAK — Label ramah untuk narasi
# ---------------------------------------------------------------------------
LABEL_ASET_BERGERAK = {
    "aset_bergerak_tabung_gas":             "tabung gas (≥5,5 kg)",
    "aset_bergerak_lemari_es":              "lemari es/kulkas",
    "aset_bergerak_ac":                     "AC (air conditioner)",
    "aset_bergerak_pemanas_air":            "pemanas air (water heater)",
    "aset_bergerak_telepon_rumah":          "telepon rumah/PSTN",
    "aset_bergerak_tv_datar":               "televisi datar",
    "aset_bergerak_emas_perhiasan":         "perhiasan emas",
    "aset_bergerak_komputer_laptop_tablet": "komputer/laptop/tablet",
    "aset_bergerak_sepeda_motor":           "sepeda motor",
    "aset_bergerak_sepeda":                 "sepeda",
    "aset_bergerak_mobil":                  "mobil",
    "aset_bergerak_perahu":                 "perahu",
    "aset_bergerak_kapal_perahu_motor":     "kapal/perahu motor",
    "aset_bergerak_smartphone":             "smartphone",
}

LABEL_ASET_TIDAK_BERGERAK = {
    "aset_tidak_bergerak_lahan_lainnya": "lahan lain selain yang dihuni",
    "aset_tidak_bergerak_rumah_lainnya": "rumah lain selain yang dihuni",
}

# ---------------------------------------------------------------------------
# TERNAK — Label untuk narasi
# ---------------------------------------------------------------------------
LABEL_TERNAK = {
    "jumlah_ternak_sapi":           "sapi",
    "jumlah_ternak_kerbau":         "kerbau",
    "jumlah_ternak_kuda":           "kuda",
    "jumlah_ternak_babi":           "babi",
    "jumlah_ternak_kambing_domba":  "kambing/domba",
}


def lookup(mapping: dict, key, default: str = "tidak diketahui") -> str:
    """Ambil label dari mapping dengan fallback aman."""
    try:
        k = int(key) if key is not None and str(key).strip() != "" else None
        return mapping.get(k, default)
    except (ValueError, TypeError):
        return default
