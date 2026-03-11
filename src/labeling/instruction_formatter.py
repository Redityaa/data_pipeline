import yaml
import json
import pandas as pd
from typing import Dict, List, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class InstructionFormatter:
    """
    Format data menjadi instruction format untuk training LLM
    Output: JSONL format (ChatML) siap untuk fine-tuning Qwen
    """
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.reasoning_template = self.config.get('reasoning_template', '')
        
    def _load_config(self, config_path: str) -> Dict:
        # PERBAIKAN 1: Memaksa encoding UTF-8 agar karakter seperti m² tidak rusak menjadi mÂ²
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def format_record(self, record: Dict) -> Dict:
        system_prompt = "Anda adalah asisten AI ahli di bidang ekonomi dan statistik sosial. Tugas Anda adalah menganalisis data rumah tangga, mengklasifikasikan tingkat kesejahteraan, dan memberikan reasoning yang masuk akal."
        
        user_content = f"{self._build_instruction()}\n\nData Rumah Tangga:\n{self._build_input_section(record)}"
        assistant_content = self._build_output_section(record)
        
        return {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
                {"role": "assistant", "content": assistant_content}
            ]
        }
    
    def _build_instruction(self) -> str:
        return """Berdasarkan data rumah tangga berikut, klasifikasikan tingkat kesejahteraan 
dan berikan reasoning yang jelas. Analisis faktor ekonomi, aset, perumahan, dan kerentanan.
Output dalam format JSON dengan field: kategori, confidence, reasoning, key_factors, recommendation."""
    
    def _build_input_section(self, record: Dict) -> str:
        money_fields = ['pendapatan', 'pendapatan_perkapita']
        boolean_fields = ['tabungan', 'air_layak', 'sanitasi_layak']
        
        input_fields = [
            'wilayah', 'pendidikan', 'pekerjaan_kepala_keluarga',
            'pendapatan', 'pendapatan_perkapita', 'tabungan',
            'motor', 'mobil', 'kulkas', 'tv', 'mesin_cuci',
            'luas_rumah', 'daya_listrik', 'status_rumah',
            'jumlah_anggota', 'usia_kepala',
            'air_layak', 'sanitasi_layak', 'overall_kelayakan_rumah'
        ]
        
        lines = []
        for field in input_fields:
            if field in record and pd.notna(record[field]):
                value = record[field]
                
                if field in money_fields:
                    formatted_value = f"Rp {value:,.0f}"
                elif field in boolean_fields:
                    formatted_value = "Ya" if value == 1 else "Tidak"
                else:
                    formatted_value = str(value)
                    
                lines.append(f"- {field}: {formatted_value}")
                
        return '\n'.join(lines)
    
    def _build_output_section(self, record: Dict) -> str:
        kategori = record.get('kategori', 'Unknown')
        
        # PERBAIKAN 2: Membulatkan angka float untuk menghemat ratusan ribu token
        confidence = round(self._calculate_confidence(record), 2)
        
        reasoning = self._generate_reasoning(record, kategori) 
        key_factors = self._extract_key_factors(record, kategori)
        
        output = {
            'kategori': kategori,
            'confidence': confidence,
            'reasoning': reasoning,
            'key_factors': key_factors,
        }
        
        return json.dumps(output, ensure_ascii=False)
    
    def _generate_reasoning(self, record: Dict, kategori: str) -> str:
        try:
            reasoning = self.reasoning_template.format(
                kategori=kategori,
                pendapatan_perkapita=record.get('pendapatan_perkapita', 0),
                
                # PERBAIKAN 3: Mengonversi 1/0 menjadi teks manusiawi untuk reasoning LLM
                tabungan="Ya" if record.get('tabungan') == 1 else "Tidak",
                
                motor=record.get('motor', 0),
                mobil=record.get('mobil', 0),
                kulkas=record.get('kulkas', 0),
                tv=record.get('tv', 0),
                mesin_cuci=record.get('mesin_cuci', 0),
                luas_rumah=record.get('luas_rumah', 0),
                daya_listrik=record.get('daya_listrik', 0),
                overall_kelayakan_rumah=record.get('overall_kelayakan_rumah', 0),
                faktor_kerentanan=self._get_faktor_kerentanan(record, kategori),
                confidence=int(self._calculate_confidence(record) * 100),
            )
        except Exception as e:
            logger.warning(f"Reasoning template error: {str(e)}")
            reasoning = f"Rumah tangga dikategorikan sebagai {kategori} berdasarkan analisis data."
        
        return reasoning
    
    def _get_faktor_kerentanan(self, record: Dict, kategori: str) -> str:
        factors = []
        pendapatan = record.get('pendapatan_perkapita', 0)
        tabungan = record.get('tabungan', 0)
        aset_kendaraan = record.get('motor', 0) + record.get('mobil', 0)
        
        kategori_lower = str(kategori).lower()
        is_upper_class = any(k in kategori_lower for k in ['atas', 'kaya', 'tinggi'])
        
        if is_upper_class:
            if pendapatan < 1500000:
                factors.append("Anomali data: Pendapatan dilaporkan sangat rendah namun tidak mencerminkan gaya hidup/aset aktual (potensi underreporting)")
            if aset_kendaraan == 0:
                factors.append("Aset mobilitas tidak tercatat, namun terkompensasi oleh indikator kesejahteraan lainnya")
            return "; ".join(factors) if factors else "Tidak ada faktor kerentanan yang mengancam stabilitas ekonomi"
            
        if tabungan == 0 or tabungan == "Tidak":
            factors.append("Tidak memiliki bantalan tabungan (sangat rentan terhadap guncangan ekonomi)")
            
        if pendapatan < 1500000:
            factors.append("Pendapatan berada di level kemiskinan ekstrem")
        elif pendapatan < 2500000:
            factors.append("Pendapatan berada di bawah garis kemiskinan standar")
            
        if aset_kendaraan == 0:
            factors.append("Ketiadaan aset kendaraan membatasi mobilitas ekonomi")
            
        return "; ".join(factors) if factors else "Tidak ada faktor kerentanan signifikan"
    
    def _extract_key_factors(self, record: Dict, kategori: str) -> List[str]:
        factors = []
        pendapatan = record.get('pendapatan_perkapita', 0)
        tabungan = record.get('tabungan', 0)
        aset_kendaraan = record.get('motor', 0) + record.get('mobil', 0)
        aset_elektronik = record.get('kulkas', 0) + record.get('tv', 0) + record.get('mesin_cuci', 0)
        kelayakan = record.get('overall_kelayakan_rumah', 0)
        
        kategori_lower = str(kategori).lower()
        is_upper_class = any(k in kategori_lower for k in ['atas', 'kaya', 'tinggi'])
        
        if is_upper_class:
            if kelayakan >= 0.7:
                factors.append('kondisi_perumahan_sangat_layak')
            if aset_elektronik >= 2:
                factors.append('kepemilikan_aset_elektronik_memadai')
            if tabungan == 1 or tabungan == "Ya":
                factors.append('memiliki_ketahanan_finansial_tabungan')
            if record.get('pendidikan', '').lower() in ['sarjana', 'diploma', 's1', 's2', 's3']:
                factors.append('pendidikan_kepala_keluarga_tinggi')
            if pendapatan < 1500000:
                factors.append('indikasi_underreported_income')
        else:
            if pendapatan < 1500000:
                factors.append('pendapatan_sangat_rendah')
            if tabungan == 0 or tabungan == "Tidak":
                factors.append('tanpa_tabungan')
            if aset_kendaraan == 0:
                factors.append('tanpa_aset_kendaraan')
            if kelayakan < 0.6:
                factors.append('kelayakan_rumah_buruk')
                
        if not factors:
            factors.append('profil_campuran_tanpa_dominasi_ekstrem')
            
        return factors
    
    def _calculate_confidence(self, record: Dict) -> float:
        base_confidence = 0.80
        
        if record.get('pendapatan_perkapita', 0) > 0:
            base_confidence += 0.03
        if record.get('tabungan', 0) >= 0:
            base_confidence += 0.02
        if record.get('overall_kelayakan_rumah', 0) > 0:
            base_confidence += 0.02
        if record.get('daya_listrik', 0) > 0:
            base_confidence += 0.01
        if record.get('luas_rumah', 0) > 0:
            base_confidence += 0.02
            
        return min(base_confidence, 0.95)

    def format_dataframe(self, df) -> List[Dict]:
        formatted_records = []
        
        for idx, row in df.iterrows():
            try:
                record = row.to_dict()
                formatted = self.format_record(record)
                formatted_records.append(formatted)
                
                if (idx + 1) % 10000 == 0:
                    logger.info(f"Formatted {idx + 1} records")
                    
            except Exception as e:
                logger.warning(f"Error formatting record {idx}: {str(e)}")
                continue
        
        logger.info(f"Formatting completed: {len(formatted_records)} records")
        return formatted_records
    
    def save_jsonl(self, records: List[Dict], output_path: str):
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        logger.info(f"Saved {len(records)} records to {output_path}")