"""Chunking helpers for documents and code."""

from __future__ import annotations

from dataclasses import dataclass
import re


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
    return [chunk.content for chunk in chunk_code_with_metadata(text, max_chars_per_chunk=max_chars)]


@dataclass(frozen=True)
class CodeChunkResult:
    content: str
    start_line: int
    end_line: int
    symbol_name: str | None
    symbol_type: str | None
    metadata: dict


_PY_CLASS_PATTERN = re.compile(r"^\s*class\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:\(|:)")
_PY_FUNCTION_PATTERN = re.compile(r"^\s*(?:async\s+def|def)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")
_TS_CLASS_PATTERN = re.compile(r"^\s*(?:export\s+)?class\s+([A-Za-z_][A-Za-z0-9_]*)\b")
_TS_FUNCTION_PATTERN = re.compile(r"^\s*(?:export\s+default\s+|export\s+)?function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")
_TS_CONST_PATTERN = re.compile(r"^\s*(?:export\s+)?(?:const|let)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=")
_MD_HEADING_PATTERN = re.compile(r"^\s{0,3}(#{1,3})\s+(.+?)\s*$")


def chunk_code_with_metadata(
    text: str,
    *,
    language: str | None = None,
    max_lines_per_chunk: int = 80,
    max_chars_per_chunk: int = 6000,
    fallback_window_lines: int = 80,
    overlap_lines: int = 10,
) -> list[CodeChunkResult]:
    if not text.strip():
        return []

    lines = text.splitlines()
    boundaries = _detect_boundaries(lines, language=language)
    chunks: list[CodeChunkResult] = []
    chunk_index = 0

    if boundaries:
        first_boundary = boundaries[0]["start"]
        if first_boundary > 0:
            preamble_lines = lines[:first_boundary]
            if "".join(preamble_lines).strip():
                chunk_index = _append_bounded_chunks(
                    chunks,
                    section_lines=preamble_lines,
                    base_start_line=1,
                    max_lines_per_chunk=max_lines_per_chunk,
                    max_chars_per_chunk=max_chars_per_chunk,
                    fallback_window_lines=fallback_window_lines,
                    overlap_lines=overlap_lines,
                    symbol_name=None,
                    symbol_type=None,
                    language=language,
                    chunk_index=chunk_index,
                    chunk_strategy="preamble",
                )
        for idx, boundary in enumerate(boundaries):
            next_start = boundaries[idx + 1]["start"] if idx + 1 < len(boundaries) else len(lines)
            section_lines = lines[boundary["start"] : next_start]
            base_start_line = boundary["start"] + 1
            chunk_index = _append_bounded_chunks(
                chunks,
                section_lines=section_lines,
                base_start_line=base_start_line,
                max_lines_per_chunk=max_lines_per_chunk,
                max_chars_per_chunk=max_chars_per_chunk,
                fallback_window_lines=fallback_window_lines,
                overlap_lines=overlap_lines,
                symbol_name=boundary["symbol_name"],
                symbol_type=boundary["symbol_type"],
                language=language,
                chunk_index=chunk_index,
                chunk_strategy="semantic",
            )
        return chunks

    return _fallback_chunks(
        lines=lines,
        language=language,
        fallback_window_lines=fallback_window_lines,
        overlap_lines=overlap_lines,
        max_chars_per_chunk=max_chars_per_chunk,
    )


def _detect_boundaries(lines: list[str], *, language: str | None) -> list[dict]:
    extension = (language or "").lower().lstrip(".")
    boundaries: list[dict] = []
    for index, line in enumerate(lines):
        detected = _detect_symbol(line, extension=extension)
        if detected is None:
            continue
        boundaries.append({"start": index, "symbol_name": detected[0], "symbol_type": detected[1]})
    return boundaries


def _detect_symbol(line: str, *, extension: str) -> tuple[str | None, str | None] | None:
    if extension == "py":
        class_match = _PY_CLASS_PATTERN.match(line)
        if class_match:
            return class_match.group(1), "class"
        function_match = _PY_FUNCTION_PATTERN.match(line)
        if function_match:
            return function_match.group(1), "function"
        return None

    if extension in {"ts", "tsx", "js", "jsx"}:
        class_match = _TS_CLASS_PATTERN.match(line)
        if class_match:
            return class_match.group(1), "class"
        function_match = _TS_FUNCTION_PATTERN.match(line)
        if function_match:
            symbol_name = function_match.group(1)
            symbol_type = "component_candidate" if extension == "tsx" and _looks_pascal_case(symbol_name) else "function"
            return symbol_name, symbol_type
        const_match = _TS_CONST_PATTERN.match(line)
        if const_match:
            symbol_name = const_match.group(1)
            symbol_type = "component_candidate" if extension == "tsx" and _looks_pascal_case(symbol_name) else "constant"
            return symbol_name, symbol_type
        return None

    if extension in {"md", "markdown"}:
        heading_match = _MD_HEADING_PATTERN.match(line)
        if heading_match:
            return heading_match.group(2).strip(), "section"
    return None


def _append_bounded_chunks(
    chunks: list[CodeChunkResult],
    *,
    section_lines: list[str],
    base_start_line: int,
    max_lines_per_chunk: int,
    max_chars_per_chunk: int,
    fallback_window_lines: int,
    overlap_lines: int,
    symbol_name: str | None,
    symbol_type: str | None,
    language: str | None,
    chunk_index: int,
    chunk_strategy: str,
) -> int:
    if not section_lines:
        return chunk_index

    if len(section_lines) <= max_lines_per_chunk and len("\n".join(section_lines)) <= max_chars_per_chunk:
        content = "\n".join(section_lines).strip()
        if content:
            chunks.append(
                CodeChunkResult(
                    content=content,
                    start_line=base_start_line,
                    end_line=base_start_line + len(section_lines) - 1,
                    symbol_name=symbol_name,
                    symbol_type=symbol_type,
                    metadata={
                        "chunk_index": chunk_index,
                        "chunk_strategy": chunk_strategy,
                        "language": language,
                        "oversized_split": False,
                    },
                )
            )
            return chunk_index + 1
        return chunk_index

    window = min(max_lines_per_chunk, fallback_window_lines)
    step = max(1, window - overlap_lines)
    for offset in range(0, len(section_lines), step):
        sliced = section_lines[offset : offset + window]
        if not sliced:
            continue
        content = "\n".join(sliced).strip()
        if not content:
            continue
        chunks.append(
            CodeChunkResult(
                content=content,
                start_line=base_start_line + offset,
                end_line=base_start_line + offset + len(sliced) - 1,
                symbol_name=symbol_name,
                symbol_type=symbol_type,
                metadata={
                    "chunk_index": chunk_index,
                    "chunk_strategy": f"{chunk_strategy}_oversized_split",
                    "language": language,
                    "oversized_split": True,
                },
            )
        )
        chunk_index += 1
        if offset + window >= len(section_lines):
            break
    return chunk_index


def _fallback_chunks(
    *,
    lines: list[str],
    language: str | None,
    fallback_window_lines: int,
    overlap_lines: int,
    max_chars_per_chunk: int,
) -> list[CodeChunkResult]:
    chunks: list[CodeChunkResult] = []
    window = max(1, fallback_window_lines)
    step = max(1, window - overlap_lines)
    chunk_index = 0
    for start in range(0, len(lines), step):
        section_lines = lines[start : start + window]
        if not section_lines:
            continue
        text = "\n".join(section_lines).strip()
        if not text:
            continue
        if len(text) > max_chars_per_chunk:
            text = text[:max_chars_per_chunk]
        chunks.append(
            CodeChunkResult(
                content=text,
                start_line=start + 1,
                end_line=start + len(section_lines),
                symbol_name=None,
                symbol_type=None,
                metadata={
                    "chunk_index": chunk_index,
                    "chunk_strategy": "fallback_window",
                    "language": language,
                    "oversized_split": False,
                },
            )
        )
        chunk_index += 1
        if start + window >= len(lines):
            break
    return chunks


def _looks_pascal_case(name: str) -> bool:
    return bool(name) and name[0].isupper() and "_" not in name
