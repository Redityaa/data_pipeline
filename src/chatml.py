"""
chatml.py
=========
Fase C: Transformasi ChatML & JSONL

Mengemas pasangan (user_content, assistant_content) menjadi struktur
messages ChatML dan menulisnya ke file .jsonl secara streaming.
"""

import json
import logging
from typing import IO

import pandas as pd

logger = logging.getLogger(__name__)


def build_chatml_record(
    user_content: str,
    assistant_content: str,
    system_prompt: str,
) -> dict:
    """
    Bangun satu record ChatML dengan format messages.

    Returns
    -------
    dict siap di-json.dumps()
    """
    return {
        "messages": [
            {
                "role": "system",
                "content": system_prompt.strip(),
            },
            {
                "role": "user",
                "content": user_content.strip(),
            },
            {
                "role": "assistant",
                "content": assistant_content.strip(),
            },
        ]
    }


def write_chunk_to_jsonl(
    df: pd.DataFrame,
    file_handle: IO,
    system_prompt: str,
) -> int:
    """
    Tulis seluruh chunk sebagai baris JSONL ke file_handle.

    Parameters
    ----------
    df            : chunk yang sudah memiliki kolom user_content & assistant_content
    file_handle   : file object (mode='a', encoding='utf-8')
    system_prompt : isi system message

    Returns
    -------
    Jumlah baris berhasil ditulis
    """
    written = 0
    for _, row in df.iterrows():
        record = build_chatml_record(
            user_content      = row["user_content"],
            assistant_content = row["assistant_content"],
            system_prompt     = system_prompt,
        )
        file_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        written += 1
    return written
