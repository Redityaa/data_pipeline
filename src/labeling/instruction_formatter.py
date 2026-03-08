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
    Output: JSONL format siap untuk fine-tuning
    """
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.reasoning_template = self.config.get('reasoning_template', '')
        
    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def format_record(self, record: Dict) -> Dict:
        """
        Format satu record menjadi instruction format
        
        Returns:
            Dict dengan format instruction-input-output
        """
        # Build instruction
        instruction = self._build_instruction()
        
        # Build input section
        input_data = self._build_input_section(record)
        
        # Build output section
        output_data = self._build_output_section(record)
        
        return {
            'instruction': instruction,
            'input': input_data,
            'output': output_data,
            'metadata': {
                'kategori': record.get('kategori'),
                'desil': record.get('desil'),
                'wealth_index': record.get('wealth_index')
            }
        }
    
    def _build_instruction(self) -> str:
        """Build instruction text"""
        return """Berdasarkan data rumah tangga berikut, klasifikasikan tingkat kesejahteraan 
dan berikan reasoning yang jelas. Analisis faktor ekonomi, aset, perumahan, dan kerentanan.
Output dalam format JSON dengan field: kategori, confidence, reasoning, key_factors, recommendation."""
    
    def _build_input_section(self, record: Dict) -> str:
        """Build input data section"""
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
                if isinstance(value, (int, float)):
                    lines.append(f"- {field}: {value:,.0f}" if field in ['pendapatan', 'pendapatan_perkapita', 'tabungan'] else f"- {field}: {value}")
                else:
                    lines.append(f"- {field}: {value}")
        
        return '\n'.join(lines)
    
    def _build_output_section(self, record: Dict) -> str:
        """Build output JSON section"""
        kategori = record.get('kategori', 'Unknown')
        confidence = self._calculate_confidence(record)
        reasoning = self._generate_reasoning(record, kategori)
        key_factors = self._extract_key_factors(record)
        
        output = {
            'kategori': kategori,
            'confidence': confidence,
            'reasoning': reasoning,
            'key_factors': key_factors,
        }
        
        return json.dumps(output, ensure_ascii=False)
    
    def _calculate_confidence(self, record: Dict) -> float:
        """Calculate confidence score based on data completeness"""
        # Simple heuristic - can be improved
        base_confidence = 0.8
        
        # Adjust based on data quality indicators
        if record.get('pendapatan_perkapita', 0) > 0:
            base_confidence += 0.05
        if record.get('tabungan', 0) >= 0:
            base_confidence += 0.05
        if record.get('overall_kelayakan_rumah', 0) > 0:
            base_confidence += 0.05
        
        return min(base_confidence, 0.95)
    
    def _generate_reasoning(self, record: Dict, kategori: str) -> str:
        """Generate reasoning text dari template"""
        try:
            reasoning = self.reasoning_template.format(
                kategori=kategori,
                pendapatan_perkapita=record.get('pendapatan_perkapita', 0),
                tabungan=record.get('tabungan', 0),
                motor=record.get('motor', 0),
                mobil=record.get('mobil', 0),
                kulkas=record.get('kulkas', 0),
                tv=record.get('tv', 0),
                mesin_cuci=record.get('mesin_cuci', 0),
                luas_rumah=record.get('luas_rumah', 0),
                daya_listrik=record.get('daya_listrik', 0),
                overall_kelayakan_rumah=record.get('overall_kelayakan_rumah', 0),
                faktor_kerentanan=self._get_faktor_kerentanan(record),
                confidence=int(self._calculate_confidence(record) * 100),
            )
        except Exception as e:
            logger.warning(f"Reasoning template error: {str(e)}")
            reasoning = f"Rumah tangga dikategorikan sebagai {kategori} berdasarkan analisis data."
        
        return reasoning
    
    def _get_faktor_kerentanan(self, record: Dict) -> str:
        """Determine vulnerability factors berdasarkan 6 kategori"""
        factors = []
        pendapatan = record.get('pendapatan_perkapita', 0)
        tabungan = record.get('tabungan', 0)
        aset_kendaraan = record.get('motor', 0) + record.get('mobil', 0)
        
        if tabungan == 0:
            factors.append("Tidak ada tabungan (rentan terhadap guncangan ekonomi)")
        
        if pendapatan < 1500000:
            factors.append("Pendapatan di bawah garis kemiskinan ekstrem")
        elif pendapatan < 2500000:
            factors.append("Pendapatan di bawah garis kemiskinan")
        elif pendapatan < 3500000:
            factors.append("Pendapatan rentan jatuh ke bawah garis kemiskinan")
        elif pendapatan < 5000000:
            factors.append("Pendapatan mendekati garis kemiskinan")
        
        if aset_kendaraan == 0:
            factors.append("Tidak memiliki kendaraan bermotor")
    
        return "; ".join(factors) if factors else "Tidak ada faktor kerentanan signifikan"
    
    def _extract_key_factors(self, record: Dict) -> List[str]:
        """Extract key factors dari record"""
        factors = []
        pendapatan = record.get('pendapatan_perkapita', 0)
        tabungan = record.get('tabungan', 0)
        aset_kendaraan = record.get('motor', 0) + record.get('mobil', 0)
        daya_listrik = record.get('daya_listrik', 0)
        kelayakan = record.get('overall_kelayakan_rumah', 0)
        
        # Faktor pendapatan
        if pendapatan < 1500000:
            factors.append('pendapatan_sangat_rendah')
        elif pendapatan < 2500000:
            factors.append('pendapatan_rendah')
        elif pendapatan < 3500000:
            factors.append('pendapatan_rentan')
        elif pendapatan < 5000000:
            factors.append('pendapatan_mendekati_garis_kemiskinan')
        
        # Faktor tabungan
        if tabungan == 0:
            factors.append('tanpa_tabungan')
        elif tabungan < 3000000:
            factors.append('tabungan_minimal')
        
        # Faktor aset
        if aset_kendaraan == 0:
            factors.append('tanpa_aset_kendaraan')
        
        # Faktor perumahan
        if daya_listrik <= 450:
            factors.append('daya_listrik_rendah')
        
        if kelayakan < 0.5:
            factors.append('kelayakan_rumah_rendah')
        
        return factors
    
    def _calculate_confidence(self, record: Dict) -> float:
        """Calculate confidence score based on data completeness"""
        base_confidence = 0.80
        
        # Adjust based on data quality indicators
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
        """
        Format seluruh DataFrame menjadi instruction format
        
        Returns:
            List of Dict dalam format instruction-input-output
        """
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
        """Save formatted records ke JSONL file"""
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        logger.info(f"Saved {len(records)} records to {output_path}")