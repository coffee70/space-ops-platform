"""Telemetry business logic service."""

import math
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, Tuple
from uuid import UUID

from sqlalchemy import case, desc, func, or_, select
from sqlalchemy.orm import Session

from app.interfaces.embedding_provider import EmbeddingProvider
from app.interfaces.llm_provider import LLMProvider
from app.models.telemetry import TelemetryChannelAlias, TelemetryData, TelemetryMetadata, TelemetryStatistics
from app.models.schemas import (
    RelatedChannel,
    StatisticsResponse,
    ExplainResponse,
    SearchResult,
)
from app.services.source_stream_service import (
    StreamIdConflictError,
    get_stream_source_id,
    normalize_source_id,
    register_stream,
    resolve_latest_stream_id,
)
from app.services.channel_alias_service import (
    get_aliases_by_telemetry_ids,
    resolve_channel_metadata,
)
from app.utils.subsystem import infer_subsystem
from app.services.realtime_service import CHANNEL_ORIGIN_CATALOG

logger = logging.getLogger(__name__)


def _compute_state(
    value: float,
    z_score: Optional[float],
    red_low: Optional[float],
    red_high: Optional[float],
    std_dev: float,
) -> Tuple[str, Optional[str]]:
    """Compute state (normal/caution/warning) and reason (out_of_limits/out_of_family)."""
    out_of_limits = False
    if red_low is not None and value < float(red_low):
        out_of_limits = True
    if red_high is not None and value > float(red_high):
        out_of_limits = True

    abs_z = abs(z_score) if z_score is not None else 0.0
    out_of_family = abs_z > 2
    caution_z = 1.5 < abs_z <= 2

    # Near limits: within 1 sigma of a limit (but not out of limits)
    near_limits = False
    rl = float(red_low) if red_low is not None else None
    rh = float(red_high) if red_high is not None else None
    if rl is not None and std_dev > 0 and rl <= value < rl + std_dev:
        near_limits = True
    if rh is not None and std_dev > 0 and rh - std_dev < value <= rh:
        near_limits = True

    if out_of_limits or out_of_family:
        state = "warning"
        reason = "out_of_limits" if out_of_limits else "out_of_family"
    elif caution_z or near_limits:
        state = "caution"
        reason = "out_of_family" if caution_z else "out_of_limits"
    else:
        state = "normal"
        reason = None
    return state, reason


class TelemetryService:
    """Service for telemetry CRUD, search, and explanation."""

    def __init__(
        self,
        db: Session,
        embedding_provider: EmbeddingProvider,
        llm_provider: LLMProvider,
    ) -> None:
        self._db = db
        self._embedding = embedding_provider
        self._llm = llm_provider

    def create_schema(
        self,
        source_id: str,
        name: str,
        units: str,
        description: Optional[str] = None,
        subsystem_tag: Optional[str] = None,
        red_low: Optional[float] = None,
        red_high: Optional[float] = None,
    ) -> UUID:
        """Create telemetry metadata with embedding."""
        logical_source_id = normalize_source_id(source_id)
        text_for_embedding = f"{name} {units} {description or ''}".strip()
        embedding = self._embedding.embed(text_for_embedding)

        meta = TelemetryMetadata(
            source_id=logical_source_id,
            name=name,
            units=units,
            description=description,
            subsystem_tag=subsystem_tag,
            channel_origin=CHANNEL_ORIGIN_CATALOG,
            red_low=Decimal(str(red_low)) if red_low is not None else None,
            red_high=Decimal(str(red_high)) if red_high is not None else None,
            embedding=embedding,
        )
        self._db.add(meta)
        self._db.flush()
        self._db.refresh(meta)
        logger.info("Created telemetry schema: %s", name)
        return meta.id

    def get_by_name(self, source_id: str, name: str) -> Optional[TelemetryMetadata]:
        """Fetch metadata by source and name."""
        return resolve_channel_metadata(self._db, source_id=source_id, channel_name=name)

    def get_by_id(self, telemetry_id: UUID) -> Optional[TelemetryMetadata]:
        """Fetch metadata by ID."""
        return self._db.get(TelemetryMetadata, telemetry_id)

    def insert_data(
        self,
        stream_id: str,
        telemetry_name: str,
        data: list[tuple[datetime, float]],
        *,
        source_id: str | None = None,
        packet_source: str | None = None,
        receiver_id: str | None = None,
    ) -> int:
        """Insert batch of time-series data. source_id scopes data when telemetry_data is source-aware."""
        logical_source_id = normalize_source_id(source_id) if source_id else get_stream_source_id(self._db, stream_id)
        if logical_source_id is None:
            raise ValueError("source_id is required for unknown stream_id")
        existing_owner = get_stream_source_id(self._db, stream_id)
        if existing_owner is not None and normalize_source_id(existing_owner) != logical_source_id:
            raise StreamIdConflictError("stream_id does not belong to source")
        meta = self.get_by_name(logical_source_id, telemetry_name)
        if not meta:
            raise ValueError(f"Telemetry not found: {telemetry_name}")
        sample_timestamps = [ts for ts, _ in data]
        register_stream(
            self._db,
            source_id=logical_source_id,
            stream_id=stream_id,
            packet_source=packet_source,
            receiver_id=receiver_id,
            started_at=min(sample_timestamps) if sample_timestamps else None,
            seen_at=max(sample_timestamps) if sample_timestamps else None,
            activate=False,
        )

        rows = [
            TelemetryData(
                stream_id=stream_id,
                telemetry_id=meta.id,
                timestamp=ts,
                sequence=index,
                value=Decimal(str(v)),
                packet_source=packet_source,
                receiver_id=receiver_id,
            )
            for index, (ts, v) in enumerate(data, start=1)
        ]
        self._db.add_all(rows)
        return len(rows)

    def semantic_search(
        self,
        query: str,
        source_id: str,
        limit: int = 10,
        subsystem: Optional[str] = None,
        anomalous_only: bool = False,
        units: Optional[str] = None,
        recent_minutes: Optional[int] = None,
    ) -> list[SearchResult]:
        """Vector similarity search with enriched metadata and optional filters."""
        if not query or not query.strip():
            return []
        data_source_id = resolve_latest_stream_id(self._db, source_id)

        # Fetch more candidates when filters are applied
        fetch_limit = limit * 5 if any([subsystem, anomalous_only, units, recent_minutes]) else limit

        logical_source_id = normalize_source_id(source_id)
        query_embedding = self._embedding.embed(query)
        distance_expr = TelemetryMetadata.embedding.cosine_distance(query_embedding)

        stmt = (
            select(TelemetryMetadata, distance_expr)
            .where(TelemetryMetadata.source_id == logical_source_id)
            .where(TelemetryMetadata.embedding.isnot(None))
            .order_by(distance_expr)
            .limit(fetch_limit)
        )
        result = self._db.execute(stmt)
        rows = result.fetchall()

        results: list[SearchResult] = []
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=recent_minutes) if recent_minutes else None
        alias_rows: list[TelemetryMetadata] = []
        candidate_ids = [meta.id for meta, _dist in rows]

        result_by_name: dict[str, SearchResult] = {}
        aliases_by_id: dict[UUID, list[str]] = get_aliases_by_telemetry_ids(
            self._db,
            source_id=source_id,
            telemetry_ids=candidate_ids,
        )

        def append_result(meta: TelemetryMetadata, match_confidence: float) -> bool:
            subsys = infer_subsystem(meta.name, meta)

            # Filter by subsystem
            if subsystem and subsys != subsystem:
                return False

            # Filter by units
            if units and meta.units != units:
                return False

            stats = self._db.get(TelemetryStatistics, (data_source_id, meta.id))
            latest = self.get_recent_value_with_timestamp(meta.name, source_id=source_id)

            # Filter by recent data
            if recent_minutes and cutoff:
                if not latest or latest[1] < cutoff:
                    return False

            current_value: Optional[float] = None
            current_status: Optional[str] = None
            last_timestamp: Optional[str] = None

            if latest and stats:
                val, ts = latest  # (value, timestamp)
                current_value = val
                last_timestamp = ts.isoformat()
                std_dev = float(stats.std_dev)
                mean = float(stats.mean)
                z_score = (val - mean) / std_dev if std_dev > 0 else None
                red_low = float(meta.red_low) if meta.red_low is not None else None
                red_high = float(meta.red_high) if meta.red_high is not None else None
                state, _ = _compute_state(val, z_score, red_low, red_high, std_dev)
                current_status = state

                if anomalous_only and state != "warning":
                    return False
            elif anomalous_only:
                # Need state for anomalous filter but we don't have stats/latest
                return False

            existing = result_by_name.get(meta.name)
            if existing is not None:
                if match_confidence <= existing.match_confidence:
                    return False
                existing.match_confidence = match_confidence
                existing.description = meta.description
                existing.subsystem_tag = subsys
                existing.units = meta.units
                existing.channel_origin = meta.channel_origin or "catalog"
                existing.discovery_namespace = meta.discovery_namespace
                existing.current_value = current_value
                existing.current_status = current_status
                existing.last_timestamp = last_timestamp
                existing.aliases = aliases_by_id.get(meta.id, [])
                return True

            result = SearchResult(
                name=meta.name,
                match_confidence=match_confidence,
                description=meta.description,
                subsystem_tag=subsys,
                units=meta.units,
                channel_origin=meta.channel_origin or "catalog",
                discovery_namespace=meta.discovery_namespace,
                current_value=current_value,
                current_status=current_status,
                last_timestamp=last_timestamp,
                aliases=aliases_by_id.get(meta.id, []),
            )
            results.append(result)
            result_by_name[meta.name] = result
            return True

        raw_query = query.strip()
        raw_query_lower = raw_query.lower()
        terms = [term for term in raw_query_lower.split() if term]
        lexical_patterns = [f"%{term}%" for term in terms] or [f"%{raw_query_lower}%"]
        lexical_clauses = [
            or_(
                TelemetryMetadata.name.ilike(pattern),
                TelemetryMetadata.description.ilike(pattern),
                TelemetryMetadata.subsystem_tag.ilike(pattern),
            )
            for pattern in lexical_patterns
        ]
        lowered_name = func.lower(TelemetryMetadata.name)
        lexical_priority = case(
            (lowered_name == raw_query_lower, 0),
            (lowered_name.like(f"{raw_query_lower}.%"), 1),
            (lowered_name.like(f"{raw_query_lower}%"), 2),
            (lowered_name.like(f"%{raw_query_lower}%"), 3),
            else_=4,
        )
        lexical_stmt = (
            select(TelemetryMetadata)
            .where(TelemetryMetadata.source_id == logical_source_id)
            .where(or_(*lexical_clauses))
            .order_by(lexical_priority, lowered_name)
            .limit(fetch_limit)
        )
        lexical_rows = self._db.execute(lexical_stmt).scalars().all()
        candidate_ids.extend(meta.id for meta in lexical_rows)

        lowered_alias = func.lower(TelemetryChannelAlias.alias_name)
        alias_priority = case(
            (lowered_alias == raw_query_lower, 0),
            (lowered_alias.like(f"{raw_query_lower}.%"), 1),
            (lowered_alias.like(f"{raw_query_lower}%"), 2),
            (lowered_alias.like(f"%{raw_query_lower}%"), 3),
            else_=4,
        )
        alias_match_ids = (
            select(
                TelemetryMetadata.id.label("telemetry_id"),
                lowered_name.label("lowered_name"),
                func.min(lowered_alias).label("lowered_alias"),
                func.min(alias_priority).label("alias_priority"),
            )
            .join(TelemetryChannelAlias, TelemetryChannelAlias.telemetry_id == TelemetryMetadata.id)
            .where(TelemetryMetadata.source_id == logical_source_id)
            .where(
                or_(
                    *[
                        TelemetryChannelAlias.alias_name.ilike(pattern)
                        for pattern in lexical_patterns
                    ]
                )
            )
            .group_by(TelemetryMetadata.id, TelemetryMetadata.name)
            .order_by("alias_priority", "lowered_alias", "lowered_name")
            .limit(fetch_limit)
            .subquery()
        )
        alias_lexical_stmt = (
            select(TelemetryMetadata)
            .join(alias_match_ids, alias_match_ids.c.telemetry_id == TelemetryMetadata.id)
            .order_by(
                alias_match_ids.c.alias_priority,
                alias_match_ids.c.lowered_alias,
                alias_match_ids.c.lowered_name,
            )
        )
        alias_rows = self._db.execute(alias_lexical_stmt).scalars().all()
        candidate_ids.extend(meta.id for meta in alias_rows)
        candidate_ids = list(dict.fromkeys(candidate_ids))
        aliases_by_id.update(
            get_aliases_by_telemetry_ids(
                self._db,
                source_id=source_id,
                telemetry_ids=candidate_ids,
            )
        )

        for meta, dist in rows:
            confidence = 1.0 - float(dist)
            if not math.isfinite(confidence):
                confidence = -1.0
            append_result(meta, confidence)

        for meta in lexical_rows:
            haystack = " ".join(
                filter(None, [meta.name.lower(), (meta.description or "").lower(), (meta.subsystem_tag or "").lower()])
            )
            if meta.name.lower() == raw_query_lower:
                lexical_confidence = 0.99
            elif raw_query_lower and raw_query_lower in meta.name.lower():
                lexical_confidence = 0.94
            else:
                term_hits = sum(term in haystack for term in terms) if terms else int(raw_query_lower in haystack)
                lexical_confidence = min(0.89, 0.5 + 0.15 * max(term_hits, 1))
            append_result(meta, lexical_confidence)

        for meta in alias_rows:
            alias_text = " ".join(aliases_by_id.get(meta.id, [])).lower()
            if raw_query_lower and raw_query_lower in alias_text:
                alias_confidence = 0.95
            else:
                term_hits = sum(term in alias_text for term in terms) if terms else int(raw_query_lower in alias_text)
                alias_confidence = min(0.9, 0.55 + 0.15 * max(term_hits, 1))
            append_result(meta, alias_confidence)

        results.sort(key=lambda result: result.match_confidence, reverse=True)
        return results[:limit]

    def get_recent_values(
        self,
        name: str,
        source_id: str,
        limit: int = 100,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> list[tuple[datetime, float]]:
        """Get most recent values for a telemetry point, optionally filtered by time range and source."""
        data_source_id = resolve_latest_stream_id(self._db, source_id)
        meta = self.get_by_name(source_id, name)
        if not meta:
            raise ValueError(f"Telemetry not found: {name}")

        stmt = (
            select(TelemetryData.timestamp, TelemetryData.value)
            .where(
                TelemetryData.telemetry_id == meta.id,
                TelemetryData.stream_id == data_source_id,
            )
            .order_by(desc(TelemetryData.timestamp), desc(TelemetryData.sequence))
            .limit(limit)
        )
        if since is not None:
            stmt = stmt.where(TelemetryData.timestamp >= since)
        if until is not None:
            stmt = stmt.where(TelemetryData.timestamp <= until)
        rows = self._db.execute(stmt).fetchall()
        return [(r[0], float(r[1])) for r in rows]

    def get_recent_value(
        self, name: str, source_id: str
    ) -> Optional[float]:
        """Get the most recent single value."""
        rows = self.get_recent_values(name, limit=1, source_id=source_id)
        return rows[0][1] if rows else None

    def get_recent_value_with_timestamp(
        self, name: str, source_id: str
    ) -> Optional[Tuple[float, datetime]]:
        """Get the most recent value and its timestamp."""
        rows = self.get_recent_values(name, limit=1, source_id=source_id)
        return (rows[0][1], rows[0][0]) if rows else None

    def get_related_channels(
        self, name: str, source_id: str, limit: int = 5
    ) -> list[RelatedChannel]:
        """Get channels linked by subsystem/physics for 'What to check next'."""
        data_source_id = resolve_latest_stream_id(self._db, source_id)
        meta = self.get_by_name(source_id, name)
        if not meta:
            return []
        canonical_name = meta.name

        subsys = infer_subsystem(meta.name, meta)
        units = meta.units or ""

        # Fetch all metadata except self
        stmt = select(TelemetryMetadata).where(
            TelemetryMetadata.source_id == normalize_source_id(source_id),
            TelemetryMetadata.name != canonical_name,
        )
        all_meta = self._db.execute(stmt).scalars().all()

        same_subsys_same_units: list[tuple[TelemetryMetadata, str]] = []
        same_subsys: list[tuple[TelemetryMetadata, str]] = []

        for m in all_meta:
            m_subsys = infer_subsystem(m.name, m)
            m_units = m.units or ""
            if m_subsys != subsys:
                continue
            if m_units == units and units:
                same_subsys_same_units.append((m, f"same subsystem and units ({subsys})"))
            elif m_units == units:
                same_subsys_same_units.append((m, f"same subsystem ({subsys})"))
            else:
                same_subsys.append((m, f"same subsystem ({subsys})"))

        # Build ordered list: same subsystem + same units first, then same subsystem
        ordered: list[tuple[TelemetryMetadata, str]] = same_subsys_same_units + same_subsys

        # If fewer than limit, add semantic search within same subsystem
        if len(ordered) < limit:
            semantic_results = self.semantic_search(canonical_name, source_id=source_id, limit=limit, subsystem=subsys)
            seen = {m.name for m, _ in ordered}
            for r in semantic_results:
                if r.name not in seen:
                    m = self.get_by_name(source_id, r.name)
                    if m:
                        ordered.append((m, f"related in {subsys}"))
                        seen.add(r.name)
                if len(ordered) >= limit:
                    break

        result: list[RelatedChannel] = []
        for m, reason in ordered[:limit]:
            current_value: Optional[float] = None
            current_status: Optional[str] = None
            last_timestamp: Optional[str] = None
            latest = self.get_recent_value_with_timestamp(m.name, source_id=source_id)
            stats = self._db.get(TelemetryStatistics, (data_source_id, m.id))
            if latest and stats:
                val, ts = latest
                current_value = val
                last_timestamp = ts.isoformat()
                std_dev = float(stats.std_dev)
                mean = float(stats.mean)
                z_score = (val - mean) / std_dev if std_dev > 0 else None
                red_low = float(m.red_low) if m.red_low is not None else None
                red_high = float(m.red_high) if m.red_high is not None else None
                state, _ = _compute_state(val, z_score, red_low, red_high, std_dev)
                current_status = state
            result.append(
                RelatedChannel(
                    name=m.name,
                    subsystem_tag=infer_subsystem(m.name, m),
                    link_reason=reason,
                    current_value=current_value,
                    current_status=current_status,
                    last_timestamp=last_timestamp,
                    units=m.units,
                )
            )
        return result

    def _compute_confidence_indicator(
        self,
        n_samples: int,
        last_timestamp: Optional[str],
    ) -> Optional[str]:
        """Compute confidence/quality indicator for the explanation."""
        if n_samples < 100:
            return "based on limited history"
        if not last_timestamp:
            return "no recent data"
        try:
            ts = datetime.fromisoformat(last_timestamp.replace("Z", "+00:00"))
            age = datetime.now(timezone.utc) - ts
            if age > timedelta(hours=1):
                return "no recent data"
        except (ValueError, TypeError):
            return "no recent data"
        return None

    def get_explanation(
        self, name: str, source_id: str, skip_llm: bool = False
    ) -> ExplainResponse:
        """Build full explanation with stats, z-score, and LLM response."""
        data_source_id = resolve_latest_stream_id(self._db, source_id)
        meta = self.get_by_name(source_id, name)
        if not meta:
            raise ValueError(f"Telemetry not found: {name}")
        canonical_name = meta.name
        aliases = get_aliases_by_telemetry_ids(
            self._db,
            source_id=source_id,
            telemetry_ids=[meta.id],
        ).get(meta.id, [])

        stats_row = self._db.get(TelemetryStatistics, (data_source_id, meta.id))
        if not stats_row:
            from app.services.statistics_service import StatisticsService

            stats_service = StatisticsService(self._db)
            stats_service._recompute_one(meta.id, source_id=data_source_id)
            self._db.flush()
            stats_row = self._db.get(TelemetryStatistics, (data_source_id, meta.id))
        recent_row = self.get_recent_value_with_timestamp(canonical_name, source_id=source_id)
        recent_value = recent_row[0] if recent_row else None  # (value, timestamp)
        last_timestamp = recent_row[1].isoformat() if recent_row else None

        if not stats_row:
            raise ValueError(f"Statistics not computed for: {name}")

        mean = float(stats_row.mean)
        std_dev = float(stats_row.std_dev)
        z_score: Optional[float] = None
        is_anomalous = False

        if recent_value is not None and std_dev > 0:
            z_score = (recent_value - mean) / std_dev
            is_anomalous = abs(z_score) > 2

        if recent_value is None:
            recent_value = mean

        red_low = float(meta.red_low) if meta.red_low is not None else None
        red_high = float(meta.red_high) if meta.red_high is not None else None
        state, state_reason = _compute_state(
            recent_value, z_score, red_low, red_high, std_dev
        )

        if skip_llm:
            llm_explanation = ""
            what_this_means = ""
            related: list = []
        else:
            prompt = (
                f"Telemetry Name: {meta.name}\n"
                f"Units: {meta.units}\n"
                f"Description: {meta.description or 'N/A'}\n"
                f"Recent Value: {recent_value}\n"
                f"Mean: {mean}\n"
                f"Std Dev: {std_dev}\n"
                f"P5: {float(stats_row.p5)}\n"
                f"P95: {float(stats_row.p95)}\n"
                f"Z-Score: {z_score if z_score is not None else 'N/A'}\n"
                f"Is Anomalous: {is_anomalous}\n\n"
                "Provide a concise explanation in two parts:\n"
                "1. WHAT THIS MEANS: Start with 1-2 sentences summarizing what this telemetry represents and whether the current value is concerning. Be direct and actionable for ops.\n"
                "2. Then add any additional context or detail if helpful."
            )

            llm_explanation = self._llm.generate(prompt)

            # Extract "What this means" as first 1-2 sentences (before first double newline or first 2 sentences)
            what_this_means = llm_explanation
            if "\n\n" in llm_explanation:
                what_this_means = llm_explanation.split("\n\n")[0].strip()
            else:
                sentences = [s.strip() for s in llm_explanation.replace("\n", " ").split(". ") if s.strip()]
                if len(sentences) >= 2:
                    s = ". ".join(sentences[:2])
                    what_this_means = s if s.endswith(".") else s + "."
                elif sentences:
                    s = sentences[0]
                    what_this_means = s if s.endswith(".") else s + "."

            related = self.get_related_channels(canonical_name, source_id=source_id, limit=5)
        n_samples = getattr(stats_row, "n_samples", 0)
        confidence = self._compute_confidence_indicator(n_samples, last_timestamp)

        return ExplainResponse(
            name=meta.name,
            aliases=aliases,
            description=meta.description,
            units=meta.units,
            channel_origin=meta.channel_origin or "catalog",
            discovery_namespace=meta.discovery_namespace,
            statistics=StatisticsResponse(
                mean=mean,
                std_dev=std_dev,
                min_value=float(stats_row.min_value),
                max_value=float(stats_row.max_value),
                p5=float(stats_row.p5),
                p50=float(stats_row.p50),
                p95=float(stats_row.p95),
                n_samples=getattr(stats_row, "n_samples", 0),
            ),
            recent_value=recent_value,
            z_score=z_score,
            is_anomalous=is_anomalous,
            state=state,
            state_reason=state_reason,
            last_timestamp=last_timestamp,
            red_low=red_low,
            red_high=red_high,
            what_this_means=what_this_means,
            what_to_check_next=related,
            confidence_indicator=confidence,
            llm_explanation=llm_explanation,
            scope=None,
        )
