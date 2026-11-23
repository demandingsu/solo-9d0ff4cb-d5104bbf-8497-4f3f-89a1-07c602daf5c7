"""大清单回归测试：2500 图版 × 400 语种清单存法下，96 MiB 地址空间内补录。

复现并看守线上问题：进程地址空间受限时，清单存法展品补录一行 CSV
因内存中展开整份笛卡尔积而 MemoryError（网页 500）。修复后合并与查询的
额外内存只随「原图版数 + 原语种数 + 本批行数」线性增长。

仅依赖标准库： python -m unittest tests.test_large_merge -v
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

from app.db import connect, init_db

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKER = os.path.join(ROOT, "tests", "large_merge_worker.py")

PLATES = 2500
LANGS = 400
MEM_LIMIT_MIB = 96


def _build_dense_fixture(db_path: str) -> None:
    """直接以 SQL 流式建出 dense 大清单展品（不经过被测代码路径）。"""
    init_db(db_path)
    conn = connect(db_path)
    try:
        plates = [("EX-BIG", f"P{i:04d}") for i in range(1, PLATES + 1)]
        langs = [("EX-BIG", f"lang{i:03d}") for i in range(1, LANGS + 1)]
        for i in range(0, len(plates), 5000):
            conn.executemany(
                "INSERT INTO dense_plates(exhibit_no, plate_no) VALUES (?, ?)",
                plates[i : i + 5000],
            )
        for i in range(0, len(langs), 5000):
            conn.executemany(
                "INSERT INTO dense_languages(exhibit_no, language) VALUES (?, ?)",
                langs[i : i + 5000],
            )
        conn.execute(
            "INSERT INTO storage_state(exhibit_no, storage_mode) VALUES ('EX-BIG', 'dense')"
        )
        conn.commit()
    finally:
        conn.close()


@unittest.skipUnless(os.name == "posix", "需要 POSIX RLIMIT_AS 限制地址空间")
class TestLargeDenseMerge(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "large.db")
        _build_dense_fixture(self.db_path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_merge_and_query_under_96mib_address_space(self):
        env = dict(os.environ)
        env["LABEL_DB_PATH"] = self.db_path
        proc = subprocess.run(
            [sys.executable, WORKER, self.db_path, str(MEM_LIMIT_MIB)],
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"子进程在 {MEM_LIMIT_MIB} MiB 地址空间内失败：\n{proc.stderr}",
        )
        out = json.loads(proc.stdout.strip().splitlines()[-1])

        # 补录 1 行缺项：清单 -> 配对，原有 100 万搭配 + 本批 1 条全部保住
        self.assertEqual(
            out["dense_to_sparse"],
            {"mode": "sparse", "switched": "dense", "new": 1, "total": PLATES * LANGS + 1},
        )
        # 配对存法查询：缺项 399（新图版只登记了 1 种语种），批次回执在案
        self.assertEqual(out["sparse_query"]["pair_count"], PLATES * LANGS + 1)
        self.assertEqual(out["sparse_query"]["missing"], LANGS - 1)
        self.assertEqual(out["sparse_query"]["batches"], 1)
        self.assertEqual(out["sparse_query"]["p2501"], ["lang001"])
        # 补齐缺项：配对 -> 清单，回执计数正确
        self.assertEqual(
            out["back_to_dense"],
            {
                "mode": "dense",
                "switched": "sparse",
                "new": LANGS - 1,
                "total": (PLATES + 1) * LANGS,
            },
        )
        # 清单存法查询：不展开整份搭配，计数与齐全性正确
        self.assertEqual(out["dense_query"]["pair_count"], (PLATES + 1) * LANGS)
        self.assertTrue(out["dense_query"]["complete"])
        self.assertTrue(out["dense_query"]["by_plate_ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
