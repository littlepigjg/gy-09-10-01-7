"""事件归档器：扫描超保留窗口的热数据 -> 聚合压缩块 -> 事务确认后删除原始行

并发正确性：
- persist_events 批量插入的事件 created_at ≈ 当前时间，必然晚于归档 cutoff（只归档
  完整的历史小时桶），所以新插入的行永不会被同轮归档选中；
- 删除只针对归档事务内快照进临时表的 id，persist_events 并发提交的新行不在其中，
  因此同一批事件不会"既归档又保留"（不重不漏）；
- 块写入与原始行删除在同一事务内提交，任何一步失败则整轮回滚，热表数据不丢。

性能（目标：单轮 50 万行 < 30s）：
- created_at 索引 + 主键 keyset 分批扫描，避免大事务/深分页；
- 聚合在内存按 (path, hour) 完成，数据库只做顺序读；
- 删除走临时表 JOIN 主键，分批提交锁粒度小，不阻塞并发插入。
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta

from sqlalchemy import select, func, tuple_, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from config import (
    EVENT_RETENTION_HOURS,
    ARCHIVE_BATCH_ROWS,
    ARCHIVE_SCAN_CHUNK,
    ARCHIVE_DELETE_CHUNK,
    DB_NAME,
)
from database import async_session
from models import RateLimitEvent, RateLimitEventArchive
from .codec import (
    BlockBuilder,
    decode_block,
    drill_distribution,
    encode_block,
    floor_hour,
    merge_payloads,
    select_granularity,
)

logger = logging.getLogger(__name__)

_TMP_TABLE = "tmp_archived_event_ids"


class ArchiveAlreadyRunning(Exception):
    """归档任务正在执行中"""


class EventArchiver:
    def __init__(self, retention_hours=EVENT_RETENTION_HOURS,
                 batch_rows=ARCHIVE_BATCH_ROWS,
                 scan_chunk=ARCHIVE_SCAN_CHUNK,
                 delete_chunk=ARCHIVE_DELETE_CHUNK):
        self.retention_hours = retention_hours
        self.batch_rows = batch_rows
        self.scan_chunk = scan_chunk
        self.delete_chunk = delete_chunk
        self._lock = asyncio.Lock()
        self.last_run = None

    def cutoff_for(self, now=None):
        """归档水位线：只归档该时间点之前（完整小时、超出保留窗口）的事件"""
        now = now or datetime.now()
        return floor_hour(now) - timedelta(hours=self.retention_hours)

    async def run_once(self):
        """执行一轮归档。返回本轮统计；若已有归档在执行则抛出 ArchiveAlreadyRunning"""
        if self._lock.locked():
            raise ArchiveAlreadyRunning("归档任务正在执行中")
        async with self._lock:
            return await self._run()

    async def _run(self):
        started = time.time()
        cutoff = self.cutoff_for()
        phases = {}
        stats = {
            "cutoff": cutoff.isoformat(),
            "retention_hours": self.retention_hours,
            "started_at": datetime.now().isoformat(),
            "rows_scanned": 0,
            "groups": 0,
            "blocks_inserted": 0,
            "blocks_updated": 0,
            "rows_deleted": 0,
            "raw_bytes": 0,
            "compressed_bytes": 0,
            "phases": phases,
        }

        async with async_session() as session:
            await session.execute(text(
                f"CREATE TEMPORARY TABLE IF NOT EXISTS {_TMP_TABLE} "
                "(id INT PRIMARY KEY) ENGINE=MEMORY"
            ))
            # 清理上轮失败可能残留的 id（同连接/同临时表复用场景）
            await session.execute(text(f"TRUNCATE TABLE {_TMP_TABLE}"))
            try:
                # ---------- 阶段1：keyset 扫描 + 内存聚合 ----------
                t0 = time.time()
                high_id = await session.scalar(
                    select(func.coalesce(func.max(RateLimitEvent.id), 0)).where(
                        RateLimitEvent.created_at < cutoff
                    )
                ) or 0

                builders: dict = {}
                last_id = 0
                remaining = self.batch_rows
                while remaining > 0 and last_id < high_id:
                    limit = min(self.scan_chunk, remaining)
                    result = await session.execute(
                        select(
                            RateLimitEvent.id,
                            RateLimitEvent.path,
                            RateLimitEvent.client_ip,
                            RateLimitEvent.algorithm,
                            RateLimitEvent.allowed,
                            RateLimitEvent.current_rate,
                            RateLimitEvent.reason,
                            RateLimitEvent.created_at,
                        )
                        .where(
                            RateLimitEvent.id > last_id,
                            RateLimitEvent.id <= high_id,
                            RateLimitEvent.created_at < cutoff,
                        )
                        .order_by(RateLimitEvent.id)
                        .limit(limit)
                    )
                    rows = result.all()
                    if not rows:
                        break

                    id_params = []
                    for (eid, path, client_ip, algorithm, allowed,
                         current_rate, reason, created_at) in rows:
                        hour = floor_hour(created_at)
                        key = (path, hour)
                        builder = builders.get(key)
                        if builder is None:
                            builder = builders[key] = BlockBuilder(hour)
                        builder.add(path, client_ip, algorithm, bool(allowed),
                                    current_rate, reason, created_at)
                        id_params.append({"id": eid})

                    await session.execute(
                        text(f"INSERT INTO {_TMP_TABLE} (id) VALUES (:id)"),
                        id_params,
                    )
                    stats["rows_scanned"] += len(id_params)
                    last_id = rows[-1][0]
                    remaining -= len(rows)
                    if len(rows) < limit:
                        break

                phases["scan_aggregate_ms"] = round((time.time() - t0) * 1000, 1)
                stats["groups"] = len(builders)

                if not builders:
                    await session.commit()
                    stats["duration_seconds"] = round(time.time() - started, 3)
                    stats["finished_at"] = datetime.now().isoformat()
                    self.last_run = stats
                    logger.info("归档扫描无超窗事件 (cutoff=%s)", cutoff)
                    return stats

                # ---------- 阶段2：压缩块分批写入（含与存量块合并） ----------
                t0 = time.time()
                existing = await self._load_existing_blocks(session, list(builders.keys()))
                values = []
                for (path, hour), builder in builders.items():
                    payload = builder.to_payload()
                    old = existing.get((path, hour))
                    if old is not None:
                        payload = merge_payloads(decode_block(old.block_data), payload)
                        raw_size = old.raw_size_bytes + builder.raw_size
                    else:
                        raw_size = builder.raw_size
                    block_data = encode_block(payload)
                    stats["raw_bytes"] += raw_size
                    stats["compressed_bytes"] += len(block_data)
                    if old is not None:
                        stats["blocks_updated"] += 1
                    else:
                        stats["blocks_inserted"] += 1
                    values.append({
                        "path": path,
                        "hour_bucket": hour,
                        "event_count": payload["n"],
                        "allowed_count": payload["a"],
                        "rejected_count": payload["r"],
                        "raw_size_bytes": raw_size,
                        "block_size": len(block_data),
                        "block_data": block_data,
                    })

                for i in range(0, len(values), 500):
                    stmt = mysql_insert(RateLimitEventArchive).values(values[i:i + 500])
                    stmt = stmt.on_duplicate_key_update(
                        event_count=stmt.inserted.event_count,
                        allowed_count=stmt.inserted.allowed_count,
                        rejected_count=stmt.inserted.rejected_count,
                        raw_size_bytes=stmt.inserted.raw_size_bytes,
                        block_size=stmt.inserted.block_size,
                        block_data=stmt.inserted.block_data,
                        updated_at=datetime.now(),
                    )
                    await session.execute(stmt)
                phases["write_blocks_ms"] = round((time.time() - t0) * 1000, 1)

                # ---------- 阶段3：确认后分批删除原始行 ----------
                t0 = time.time()
                while True:
                    result = await session.execute(text(
                        f"DELETE FROM rate_limit_events WHERE id IN "
                        f"(SELECT id FROM (SELECT id FROM {_TMP_TABLE} LIMIT :c) AS archived_ids)"
                    ), {"c": self.delete_chunk})
                    deleted = result.rowcount or 0
                    stats["rows_deleted"] += deleted
                    await session.execute(
                        text(f"DELETE FROM {_TMP_TABLE} LIMIT :c"),
                        {"c": self.delete_chunk},
                    )
                    if deleted < self.delete_chunk:
                        break
                phases["delete_ms"] = round((time.time() - t0) * 1000, 1)

                await session.commit()
            finally:
                try:
                    await session.execute(text(f"DROP TEMPORARY TABLE IF EXISTS {_TMP_TABLE}"))
                    await session.commit()
                except Exception:
                    pass

        stats["duration_seconds"] = round(time.time() - started, 3)
        stats["finished_at"] = datetime.now().isoformat()
        stats["rows_per_second"] = round(
            stats["rows_scanned"] / stats["duration_seconds"]
        ) if stats["duration_seconds"] else 0
        self.last_run = stats
        logger.info(
            "归档完成: 扫描=%s 删除=%s 块(新增=%s 更新=%s) 压缩比=%.1fx 用时=%ss",
            stats["rows_scanned"], stats["rows_deleted"],
            stats["blocks_inserted"], stats["blocks_updated"],
            (stats["raw_bytes"] / stats["compressed_bytes"])
            if stats["compressed_bytes"] else 0,
            stats["duration_seconds"],
        )
        return stats

    async def _load_existing_blocks(self, session, keys):
        """批量加载同 (path, hour) 的存量块，用于块合并/幂等重跑"""
        existing = {}
        for i in range(0, len(keys), 200):
            result = await session.execute(
                select(RateLimitEventArchive).where(
                    tuple_(
                        RateLimitEventArchive.path,
                        RateLimitEventArchive.hour_bucket,
                    ).in_(keys[i:i + 200])
                )
            )
            for row in result.scalars():
                existing[(row.path, row.hour_bucket)] = row
        return existing

    # ==================== 归档查询（供页面/API） ====================

    async def summary_by_path(self, session):
        """各路径归档量与压缩比、可下钻区间"""
        result = await session.execute(
            select(
                RateLimitEventArchive.path,
                func.count(RateLimitEventArchive.id).label("blocks"),
                func.sum(RateLimitEventArchive.event_count).label("event_count"),
                func.sum(RateLimitEventArchive.allowed_count).label("allowed_count"),
                func.sum(RateLimitEventArchive.rejected_count).label("rejected_count"),
                func.sum(RateLimitEventArchive.raw_size_bytes).label("raw_bytes"),
                func.sum(RateLimitEventArchive.block_size).label("compressed_bytes"),
                func.min(RateLimitEventArchive.hour_bucket).label("min_hour"),
                func.max(RateLimitEventArchive.hour_bucket).label("max_hour"),
            ).group_by(RateLimitEventArchive.path)
            .order_by(func.sum(RateLimitEventArchive.event_count).desc())
        )
        rows = result.all()
        return [{
            "path": r.path,
            "blocks": r.blocks,
            "event_count": int(r.event_count or 0),
            "allowed_count": int(r.allowed_count or 0),
            "rejected_count": int(r.rejected_count or 0),
            "raw_bytes": int(r.raw_bytes or 0),
            "compressed_bytes": int(r.compressed_bytes or 0),
            "compression_ratio": round(r.raw_bytes / r.compressed_bytes, 2)
            if r.compressed_bytes else None,
            "drill_range_start": r.min_hour.isoformat() if r.min_hour else None,
            # 区间右端为最后一个小时桶的下一小时（半开区间）
            "drill_range_end": (r.max_hour + timedelta(hours=1)).isoformat()
            if r.max_hour else None,
        } for r in rows]

    async def archive_totals(self, session):
        result = await session.execute(
            select(
                func.count(RateLimitEventArchive.id),
                func.coalesce(func.sum(RateLimitEventArchive.event_count), 0),
                func.coalesce(func.sum(RateLimitEventArchive.raw_size_bytes), 0),
                func.coalesce(func.sum(RateLimitEventArchive.block_size), 0),
                func.min(RateLimitEventArchive.hour_bucket),
                func.max(RateLimitEventArchive.hour_bucket),
            )
        )
        blocks, events, raw, comp, min_hour, max_hour = result.one()
        return {
            "blocks": blocks,
            "event_count": int(events),
            "raw_bytes": int(raw),
            "compressed_bytes": int(comp),
            "compression_ratio": round(raw / comp, 2) if comp else None,
            "range_start": min_hour.isoformat() if min_hour else None,
            "range_end": (max_hour + timedelta(hours=1)).isoformat() if max_hour else None,
        }

    async def hot_event_count(self, session):
        """热表行数估算（information_schema 近似值，避免大表 COUNT(*)）"""
        row = await session.execute(text(
            "SELECT TABLE_ROWS FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = :db AND TABLE_NAME = 'rate_limit_events'"
        ), {"db": DB_NAME})
        value = row.scalar()
        if value is None:
            value = await session.scalar(
                select(func.count(RateLimitEvent.id))
            )
        return int(value or 0)

    async def drill(self, session, path, start, end, granularity=None):
        """按时间范围读取压缩块并下钻还原近似分布"""
        if granularity is None:
            granularity = select_granularity(start, end)
        result = await session.execute(
            select(RateLimitEventArchive.hour_bucket, RateLimitEventArchive.block_data)
            .where(
                RateLimitEventArchive.path == path,
                RateLimitEventArchive.hour_bucket >= floor_hour(start),
                RateLimitEventArchive.hour_bucket <= floor_hour(end),
            )
            .order_by(RateLimitEventArchive.hour_bucket)
        )
        hour_blocks = [
            (hour_bucket, decode_block(block_data))
            for hour_bucket, block_data in result.all()
        ]
        return drill_distribution(hour_blocks, start, end, granularity)


_archiver: EventArchiver | None = None


def get_archiver() -> EventArchiver:
    global _archiver
    if _archiver is None:
        _archiver = EventArchiver()
    return _archiver
