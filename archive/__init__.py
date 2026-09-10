"""rate_limit_events 冷热分层归档

- codec:    (path, hour) 压缩块的聚合/编解码/时间范围下钻（纯标准库，可独立测试）
- archiver: 归档扫描、块写入、事务确认后删除原始行
"""
from .archiver import EventArchiver, get_archiver
from .codec import (
    BlockBuilder,
    decode_block,
    drill_distribution,
    encode_block,
    estimate_row_size,
    floor_hour,
    merge_payloads,
    select_granularity,
)

__all__ = [
    "EventArchiver",
    "get_archiver",
    "BlockBuilder",
    "decode_block",
    "drill_distribution",
    "encode_block",
    "estimate_row_size",
    "floor_hour",
    "merge_payloads",
    "select_granularity",
]
