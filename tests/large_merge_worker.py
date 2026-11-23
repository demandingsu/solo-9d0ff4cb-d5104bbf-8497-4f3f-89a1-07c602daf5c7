"""大清单回归测试的子进程工作脚本（被 tests.test_large_merge 调用）。

用法：python tests/large_merge_worker.py <db_path> <mem_limit_mib>
在指定地址空间限制下，对 2500 图版 × 400 语种的清单存法展品做补录、
存法切换与查询，结果以 JSON 打印到 stdout。
"""

from __future__ import annotations

import json
import resource
import sys

limit_mib = int(sys.argv[2])
resource.setrlimit(resource.RLIMIT_AS, (limit_mib * 1024 * 1024, limit_mib * 1024 * 1024))

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from app import storage  # noqa: E402
from app.db import connect  # noqa: E402

EXHIBIT = "EX-BIG"


def main() -> None:
    conn = connect(sys.argv[1])
    out: dict[str, dict] = {}

    # 1) 清单存法 2500×400，补录 1 行缺项 -> 切配对存法，保住全部搭配
    r = storage.merge_and_store(conn, EXHIBIT, [("P2501", "lang001")])
    out["dense_to_sparse"] = {
        "mode": r.storage_mode,
        "switched": r.switched_from,
        "new": r.new_pairs,
        "total": r.total_pairs,
    }

    # 2) 配对存法下查询详情：不物化配对元组大集合
    d = storage.get_exhibit(conn, EXHIBIT)
    out["sparse_query"] = {
        "pair_count": d["pair_count"],
        "missing": len(d["missing_pairs"]),
        "batches": len(d["batches"]),
        "p2501": d["by_plate"].get("P2501"),
    }

    # 3) 补齐 399 个缺项 -> 切回清单存法
    r = storage.merge_and_store(
        conn, EXHIBIT, [("P2501", f"lang{i:03d}") for i in range(2, 401)]
    )
    out["back_to_dense"] = {
        "mode": r.storage_mode,
        "switched": r.switched_from,
        "new": r.new_pairs,
        "total": r.total_pairs,
    }

    # 4) 清单存法下查询详情：不展开整份搭配
    d = storage.get_exhibit(conn, EXHIBIT)
    out["dense_query"] = {
        "pair_count": d["pair_count"],
        "complete": d["is_complete"],
        "by_plate_ok": d["by_plate"]["P0001"] == d["languages"],
    }

    conn.close()
    print(json.dumps(out))


if __name__ == "__main__":
    main()
