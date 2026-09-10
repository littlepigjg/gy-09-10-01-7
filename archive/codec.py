"""归档压缩块：按 (path, hour) 聚合的事件分布

块载荷（JSON + zlib）在保留"近似分布"的同时大幅压缩原始行：
- 小时内按分钟稀疏记录 total/allowed/rejected 与平均速率 -> 支持任意时间范围下钻还原
- 块级保留算法/拒绝原因/Top IP 计数
- IP 等高基数字典设有上限，超限截断并打 ipc 标记（下钻结果据此标注近似）
"""
import json
import zlib
from collections import defaultdict
from datetime import datetime, timedelta

BLOCK_VERSION = 1
ZLIB_LEVEL = 6

# 块级 Top IP 保留条数；单分钟明细超过该事件量后不再保留分钟级 IP 明细
TOP_IPS_PER_BLOCK = 50
TOP_IPS_PER_MINUTE = 10
MINUTE_IP_DETAIL_LIMIT = 1000
# 原始行估算固定开销（InnoDB 行头 + 固定列 + NULL 位图的近似值）
ROW_FIXED_OVERHEAD = 52


def floor_hour(dt: datetime) -> datetime:
    """对齐到整点小时桶（naive/aware 均按其自身时区对齐）"""
    return dt.replace(minute=0, second=0, microsecond=0)


def estimate_row_size(path="", client_ip=None, algorithm=None, reason=None) -> int:
    """估算单条 rate_limit_events 原始行的落库字节数（用于压缩比统计）"""
    return (
        ROW_FIXED_OVERHEAD
        + len((path or "").encode("utf-8"))
        + len((client_ip or "").encode("utf-8"))
        + len((algorithm or "").encode("utf-8"))
        + len((reason or "").encode("utf-8"))
    )


class BlockBuilder:
    """收集同一 (path, hour) 的事件，产出压缩块载荷"""

    def __init__(self, hour_bucket: datetime):
        self.hour_bucket = floor_hour(hour_bucket)
        self.total = 0
        self.allowed = 0
        self.rejected = 0
        self.rate_sum = 0.0
        self.rate_n = 0
        self.algorithms: dict = defaultdict(int)
        self.reasons: dict = defaultdict(int)
        self.ips: dict = defaultdict(int)
        self.ip_capped = False
        # minute_offset -> 分钟统计
        self.minutes: dict = {}
        self.raw_size = 0

    def add(self, path, client_ip, algorithm, allowed, current_rate, reason, created_at):
        self.total += 1
        if allowed:
            self.allowed += 1
        else:
            self.rejected += 1
            if reason:
                self.reasons[reason] += 1
        if algorithm:
            self.algorithms[algorithm] += 1
        if client_ip:
            if client_ip not in self.ips and len(self.ips) >= TOP_IPS_PER_BLOCK:
                self.ip_capped = True
            self.ips[client_ip] += 1
        if current_rate is not None:
            self.rate_sum += float(current_rate)
            self.rate_n += 1

        minute = created_at.minute
        m = self.minutes.get(minute)
        if m is None:
            m = {"t": 0, "a": 0, "r": 0, "rs": 0.0, "rn": 0, "ip": defaultdict(int)}
            self.minutes[minute] = m
        m["t"] += 1
        if allowed:
            m["a"] += 1
        else:
            m["r"] += 1
        if current_rate is not None:
            m["rs"] += float(current_rate)
            m["rn"] += 1
        if client_ip and m["t"] <= MINUTE_IP_DETAIL_LIMIT:
            m["ip"][client_ip] += 1

        self.raw_size += estimate_row_size(path, client_ip, algorithm, reason)

    def to_payload(self) -> dict:
        minutes_out = {}
        for minute, m in sorted(self.minutes.items()):
            entry = {"t": m["t"], "a": m["a"], "r": m["r"]}
            if m["rn"]:
                entry["rs"] = round(m["rs"], 3)
                entry["rn"] = m["rn"]
            if m["t"] <= MINUTE_IP_DETAIL_LIMIT and m["ip"]:
                top = sorted(m["ip"].items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_IPS_PER_MINUTE]
                entry["ip"] = dict(top)
                if len(m["ip"]) > TOP_IPS_PER_MINUTE:
                    entry["ipc"] = True
            minutes_out[str(minute)] = entry

        payload = {
            "v": BLOCK_VERSION,
            "n": self.total,
            "a": self.allowed,
            "r": self.rejected,
            "alg": dict(sorted(self.algorithms.items())),
            "m": minutes_out,
        }
        if self.reasons:
            payload["rsn"] = dict(sorted(self.reasons.items()))
        if self.ips:
            top = sorted(self.ips.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_IPS_PER_BLOCK]
            payload["ip"] = dict(top)
        if self.ip_capped:
            payload["ipc"] = True
        if self.rate_n:
            payload["rs"] = round(self.rate_sum, 3)
            payload["rn"] = self.rate_n
        return payload


def _top_ips(counter: dict, limit: int = TOP_IPS_PER_BLOCK):
    return sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]


def merge_payloads(p1: dict, p2: dict) -> dict:
    """合并两个同 (path, hour) 的块载荷（归档块重写/补录时使用）"""
    out = dict(p1)
    out["n"] = p1.get("n", 0) + p2.get("n", 0)
    out["a"] = p1.get("a", 0) + p2.get("a", 0)
    out["r"] = p1.get("r", 0) + p2.get("r", 0)
    out["rs"] = round(p1.get("rs", 0.0) + p2.get("rs", 0.0), 3)
    out["rn"] = p1.get("rn", 0) + p2.get("rn", 0)
    if not out["rn"]:
        out.pop("rs", None)
        out.pop("rn", None)

    for key in ("alg", "rsn", "ip"):
        merged = dict(p1.get(key, {}))
        for k, v in p2.get(key, {}).items():
            merged[k] = merged.get(k, 0) + v
        if merged:
            if key == "ip":
                out[key] = dict(_top_ips(merged))
            else:
                out[key] = dict(sorted(merged.items()))
        else:
            out.pop(key, None)

    minutes = dict(p1.get("m", {}))
    for mk, mv2 in p2.get("m", {}).items():
        mv1 = minutes.get(mk)
        if mv1 is None:
            minutes[mk] = dict(mv2)
            continue
        merged = {
            "t": mv1.get("t", 0) + mv2.get("t", 0),
            "a": mv1.get("a", 0) + mv2.get("a", 0),
            "r": mv1.get("r", 0) + mv2.get("r", 0),
            "rs": round(mv1.get("rs", 0.0) + mv2.get("rs", 0.0), 3),
            "rn": mv1.get("rn", 0) + mv2.get("rn", 0),
        }
        if not merged["rn"]:
            merged.pop("rs", None)
            merged.pop("rn", None)
        ip_merged = dict(mv1.get("ip", {}))
        for k, v in mv2.get("ip", {}).items():
            ip_merged[k] = ip_merged.get(k, 0) + v
        if ip_merged:
            merged["ip"] = dict(
                sorted(ip_merged.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_IPS_PER_MINUTE]
            )
        if mv1.get("ipc") or mv2.get("ipc") or len(ip_merged) > TOP_IPS_PER_MINUTE:
            merged["ipc"] = True
        minutes[mk] = merged
    out["m"] = dict(sorted(minutes.items(), key=lambda kv: int(kv[0])))

    out["ipc"] = bool(p1.get("ipc") or p2.get("ipc"))
    out["v"] = BLOCK_VERSION
    return out


def encode_block(payload: dict) -> bytes:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return zlib.compress(raw, ZLIB_LEVEL)


def decode_block(data: bytes) -> dict:
    return json.loads(zlib.decompress(data).decode("utf-8"))


# ==================== 时间范围下钻 ====================

_GRANULARITIES = ("minute", "5min", "hour")
_GRANULARITY_DELTA = {
    "minute": timedelta(minutes=1),
    "5min": timedelta(minutes=5),
    "hour": timedelta(hours=1),
}


def select_granularity(start: datetime, end: datetime) -> str:
    """根据时间跨度自动选择下钻粒度：<=6h 按分钟，<=3d 按5分钟，否则按小时"""
    span = (end - start).total_seconds()
    if span <= 6 * 3600:
        return "minute"
    if span <= 3 * 24 * 3600:
        return "5min"
    return "hour"


def _point_key(hour_bucket: datetime, minute: int, granularity: str) -> datetime:
    if granularity == "hour":
        return hour_bucket
    if granularity == "5min":
        return hour_bucket.replace(minute=(minute // 5) * 5)
    return hour_bucket.replace(minute=minute)


def drill_distribution(hour_blocks, start: datetime, end: datetime, granularity=None):
    """按时间范围下钻还原近似分布。

    hour_blocks: 可迭代的 (hour_bucket(datetime), payload(dict))，块可覆盖范围外的相邻小时
    返回按粒度聚合的时间点序列，以及范围内的 Top IP / 原因 / 算法分布。
    落在范围边界上的小时块按分钟裁剪，对应数据点标记 partial=True。
    """
    if granularity is None:
        granularity = select_granularity(start, end)
    if granularity not in _GRANULARITIES:
        raise ValueError(f"不支持的粒度: {granularity}")

    points: dict = {}
    ip_counter: dict = defaultdict(int)
    reason_counter: dict = defaultdict(int)
    alg_counter: dict = defaultdict(int)
    total = allowed = rejected = 0
    rate_sum = 0.0
    rate_n = 0
    ip_capped = False

    def point(bucket):
        p = points.get(bucket)
        if p is None:
            p = {"bucket": bucket, "total": 0, "allowed": 0, "rejected": 0,
                 "rs": 0.0, "rn": 0, "partial": False}
            points[bucket] = p
        return p

    for hour_bucket, payload in hour_blocks:
        hour_end = hour_bucket + timedelta(hours=1)
        if hour_end <= start or hour_bucket >= end:
            continue
        # 整块在范围内时，原因/算法/TopIP 等块级维度精确纳入
        in_range = start <= hour_bucket and hour_end <= end
        if in_range:
            for k, v in payload.get("ip", {}).items():
                ip_counter[k] += v
            for k, v in payload.get("rsn", {}).items():
                reason_counter[k] += v
            for k, v in payload.get("alg", {}).items():
                alg_counter[k] += v
            total += payload.get("n", 0)
            allowed += payload.get("a", 0)
            rejected += payload.get("r", 0)
            rate_sum += payload.get("rs", 0.0)
            rate_n += payload.get("rn", 0)
            if payload.get("ipc"):
                ip_capped = True

        for mk, mv in payload.get("m", {}).items():
            minute_dt = hour_bucket.replace(minute=int(mk))
            minute_end = minute_dt + timedelta(minutes=1)
            if minute_end <= start or minute_dt >= end:
                continue
            p = point(_point_key(hour_bucket, int(mk), granularity))
            p["total"] += mv.get("t", 0)
            p["allowed"] += mv.get("a", 0)
            p["rejected"] += mv.get("r", 0)
            p["rs"] += mv.get("rs", 0.0)
            p["rn"] += mv.get("rn", 0)
            # minute 粒度按分钟精确裁剪；5min/hour 桶被范围边界切断时该点为近似值
            if granularity != "minute":
                bucket_start = _point_key(hour_bucket, int(mk), granularity)
                bucket_end = bucket_start + _GRANULARITY_DELTA[granularity]
                if bucket_start < start or bucket_end > end:
                    p["partial"] = True
            if not in_range:
                # 边界裁剪块：分钟数据参与曲线，同时把可精确归因的部分计入汇总
                total += mv.get("t", 0)
                allowed += mv.get("a", 0)
                rejected += mv.get("r", 0)
                rate_sum += mv.get("rs", 0.0)
                rate_n += mv.get("rn", 0)
                for k, v in mv.get("ip", {}).items():
                    ip_counter[k] += v
                if mv.get("ipc"):
                    ip_capped = True
                # 边界块的块级 TopIP 未参与统计，分钟明细仅覆盖热点 IP，结果近似
                ip_capped = True

    series = []
    for bucket in sorted(points):
        p = points[bucket]
        item = {
            "bucket": bucket.isoformat(),
            "total": p["total"],
            "allowed": p["allowed"],
            "rejected": p["rejected"],
            "avg_rate": round(p["rs"] / p["rn"], 2) if p["rn"] else None,
        }
        if p["partial"]:
            item["partial"] = True
        series.append(item)

    return {
        "granularity": granularity,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "total": total,
        "allowed": allowed,
        "rejected": rejected,
        "avg_rate": round(rate_sum / rate_n, 2) if rate_n else None,
        "points": series,
        "top_ips": [{"ip": ip, "count": cnt} for ip, cnt in _top_ips(ip_counter, 10)],
        "reasons": dict(sorted(reason_counter.items(), key=lambda kv: -kv[1])),
        "algorithms": dict(sorted(alg_counter.items())),
        # 由有损压缩块还原的分布恒为近似值；ipc 表示 Top IP 经过截断
        "approx": True,
        "ip_truncated": ip_capped,
    }
