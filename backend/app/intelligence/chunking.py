"""Chunking helpers for documents and code."""

from __future__ import annotations


def chunk_text(text: str, *, max_chars: int = 1200, overlap: int = 120) -> list[str]:
    cleaned = text.strip()
    if not cleaned:
        return []
    chunks: list[str] = []
    start = 0
    while start < len(cleaned):
        end = min(len(cleaned), start + max_chars)
        chunks.append(cleaned[start:end])
        if end >= len(cleaned):
            break
        start = max(0, end - overlap)
    return chunks


def chunk_code(text: str, *, max_chars: int = 1500) -> list[str]:
    return chunk_text(text, max_chars=max_chars, overlap=200)
