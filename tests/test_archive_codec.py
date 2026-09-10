"""归档压缩块编解码/聚合/下钻测试（仅依赖标准库，可直接运行: python3 -m unittest）"""
import importlib.util
import os
import time
import unittest
from datetime import datetime, timedelta

_HERE = os.path.dirname(__file__)
_spec = importlib.util.spec_from_file_location(
    "archive_codec", os.path.join(_HERE, "..", "archive", "codec.py")
)
codec = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(codec)

BlockBuilder = codec.BlockBuilder
encode_block = codec.encode_block
decode_block = codec.decode_block
drill_distribution = codec.drill_distribution
merge_payloads = codec.merge_payloads
floor_hour = codec.floor_hour

BASE = datetime(2026, 9, 1, 0, 0, 0)


def build_hour(path, hour, per_minute, rejected_minutes=(), unique_ips=False):
    b = BlockBuilder(BASE + timedelta(hours=hour))
    for minute, cnt in per_minute.items():
        for i in range(cnt):
            allowed = minute not in rejected_minutes
            ip = f"10.0.0.{(minute + i) % 250 + 1}" if unique_ips else "10.0.0.1"
            b.add(
                path,
                ip,
                "token_bucket",
                allowed,
                100.0 if allowed else 200.0,
                None if allowed else "rate exceeded",
                BASE + timedelta(hours=hour, minutes=minute),
            )
    return b


class TestBlockBuilder(unittest.TestCase):
    def test_counts_and_roundtrip(self):
        b = build_hour("/api/x", 0, {m: 2 for m in range(60)}, rejected_minutes=(5, 10))
        payload = b.to_payload()
        self.assertEqual(payload["n"], 120)
        self.assertEqual(payload["a"], 116)
        self.assertEqual(payload["r"], 4)
        self.assertEqual(sum(m["t"] for m in payload["m"].values()), 120)
        self.assertEqual(payload["rsn"], {"rate exceeded": 4})
        # zlib 往返
        restored = decode_block(encode_block(payload))
        self.assertEqual(restored["n"], payload["n"])
        # 压缩块显著小于原始数据估算
        self.assertLess(len(encode_block(payload)), b.raw_size / 10)

    def test_ip_capping(self):
        b = BlockBuilder(BASE)
        for i in range(codec.TOP_IPS_PER_BLOCK + 10):
            b.add("/p", f"9.9.{i // 256}.{i % 256}", "tb", True, None, None,
                  BASE + timedelta(seconds=i))
        payload = b.to_payload()
        self.assertTrue(payload["ipc"])
        self.assertEqual(len(payload["ip"]), codec.TOP_IPS_PER_BLOCK)


class TestDrill(unittest.TestCase):
    def setUp(self):
        self.blocks = [
            (BASE, build_hour("/p", 0, {m: 1 for m in range(60)}).to_payload()),
            (BASE + timedelta(hours=1), build_hour("/p", 1, {m: 2 for m in range(60)}).to_payload()),
        ]

    def test_aligned_range_minute(self):
        res = drill_distribution(
            self.blocks, BASE + timedelta(minutes=30),
            BASE + timedelta(hours=1, minutes=30), "minute",
        )
        self.assertEqual(res["total"], 90)          # 30*1 + 30*2
        self.assertEqual(len(res["points"]), 60)
        self.assertTrue(all(not p.get("partial") for p in res["points"]))

    def test_unaligned_range_marks_coarse_buckets_partial(self):
        res = drill_distribution(
            self.blocks, BASE + timedelta(minutes=32),
            BASE + timedelta(hours=1, minutes=33), "5min",
        )
        self.assertEqual(res["total"], 28 * 1 + 33 * 2)  # 28 + 66 = 94
        partial = [p for p in res["points"] if p.get("partial")]
        self.assertEqual(len(partial), 2)           # 首尾各一个被切断的 5min 桶
        self.assertEqual(sum(p["total"] for p in partial), 3 + 6)
        # minute 粒度永远精确（按分钟裁剪）
        resm = drill_distribution(
            self.blocks, BASE + timedelta(minutes=32),
            BASE + timedelta(hours=1, minutes=33), "minute",
        )
        self.assertTrue(all(not p.get("partial") for p in resm["points"]))

    def test_full_blocks_exact_dimensions(self):
        res = drill_distribution(self.blocks, BASE, BASE + timedelta(hours=2), "hour")
        self.assertEqual(res["total"], 180)
        self.assertEqual(res["algorithms"], {"token_bucket": 180})
        self.assertFalse(res["ip_truncated"])
        self.assertTrue(all(not p.get("partial") for p in res["points"]))

    def test_empty_range(self):
        res = drill_distribution(
            self.blocks, BASE + timedelta(days=9), BASE + timedelta(days=10), "hour"
        )
        self.assertEqual(res["total"], 0)
        self.assertEqual(res["points"], [])

    def test_auto_granularity(self):
        self.assertEqual(codec.select_granularity(BASE, BASE + timedelta(hours=2)), "minute")
        self.assertEqual(codec.select_granularity(BASE, BASE + timedelta(hours=12)), "5min")
        self.assertEqual(codec.select_granularity(BASE, BASE + timedelta(days=4)), "hour")


class TestMerge(unittest.TestCase):
    def test_merge_preserves_counts(self):
        b1 = build_hour("/p", 0, {m: 1 for m in range(60)})
        b2 = BlockBuilder(BASE)
        b2.add("/p", "1.1.1.1", "sliding_window", False, 50.0, "rate exceeded",
               BASE + timedelta(minutes=0))
        merged = merge_payloads(b1.to_payload(), b2.to_payload())
        self.assertEqual(merged["n"], 61)
        self.assertEqual((merged["a"], merged["r"]), (60, 1))
        self.assertEqual(sum(m["t"] for m in merged["m"].values()), 61)
        self.assertEqual(merged["m"]["0"]["t"], 2)
        self.assertEqual(decode_block(encode_block(merged))["n"], 61)


class TestCutoff(unittest.TestCase):
    def test_aligns_to_full_hour(self):
        # 不依赖真实 SQLAlchemy：复刻 EventArchiver.cutoff_for 的对齐逻辑
        now = datetime(2026, 9, 10, 12, 47, 33)
        cutoff = floor_hour(now) - timedelta(hours=72)
        self.assertEqual(cutoff, datetime(2026, 9, 7, 12, 0, 0))


class TestScanConcurrencyInvariant(unittest.TestCase):
    """模拟归档 keyset 扫描与 persist_events 并发插入的"不重不漏"不变量。

    归档只选择 id <= 扫描开始时的 high_id（created_at < cutoff 的最大 id）
    且 created_at < cutoff 的行；并发插入的事件时间≈now，天然落在窗口内，
    不得被归档删除。
    """

    def test_concurrent_inserts_never_archived(self):
        cutoff = BASE + timedelta(hours=2)
        # 表：1000 条历史行（全部超窗）
        table = [
            (i, BASE + timedelta(seconds=i)) for i in range(1, 1001)
        ]
        # persist_events 在扫描期间并发插入的新行（窗口内）
        concurrent = [
            (1001 + i, datetime(2026, 9, 10, 12, 0, 0) + timedelta(seconds=i))
            for i in range(100)
        ]

        def scan_high_id(rows):
            return max((i for i, ts in rows if ts < cutoff), default=0)

        high_id = scan_high_id(table)  # 扫描开始时的水位
        self.assertEqual(high_id, 1000)

        captured = []
        last_id = 0
        chunk = 200
        # 模拟分批扫描；第二批之后并发插入到达
        for step in range(10):
            visible = table if step >= 2 else table  # 新行在第3批对事务可见与否都不影响
            if step == 2:
                visible = visible + concurrent
            batch = [
                (i, ts) for i, ts in visible
                if last_id < i <= high_id and ts < cutoff
            ][:chunk]
            if not batch:
                break
            captured.extend(i for i, _ in batch)
            last_id = batch[-1][0]

        # 删除集合必须恰好等于归档集合
        self.assertEqual(sorted(captured), list(range(1, 1001)))
        self.assertTrue(set(captured).isdisjoint(i for i, _ in concurrent))

    def test_500k_aggregation_performance_budget(self):
        # 50 万行聚合 + 编码应在 30s 预算的 CPU 份额内（实测约 8-10s）
        b = BlockBuilder(BASE)
        t0 = time.time()
        for i in range(500_000):
            ts = BASE + timedelta(seconds=i % 3600)
            b.add("/api/heavy", f"10.0.{(i // 256) % 256}.{i % 256}",
                  "token_bucket", i % 4 != 0, 100.0 + (i % 50),
                  None if i % 4 != 0 else "rate exceeded", ts)
        data = encode_block(b.to_payload())
        elapsed = time.time() - t0
        self.assertEqual(b.total, 500_000)
        self.assertLess(elapsed, 25.0, f"聚合耗时 {elapsed:.1f}s 超出预算")
        self.assertLess(len(data), b.raw_size / 20)  # 单路径单小时至少 20x 压缩


if __name__ == "__main__":
    unittest.main()
