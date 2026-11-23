"""展品图版-语种搭配的登记与查询。

存法（每批按与库内已登记数据 *合并后* 的结果重新选择）：

* dense（清单存法）：当某展品的图版集合与语种集合构成完整笛卡尔积，
  即任意图版都能配任意语种时，只保留去重后的图版清单与语种清单两张表；
* sparse（配对存法）：存在缺项时，只保留实际登记的 (图版, 语种) 配对。

两种存法随时可以互换：批次合并后重新判定，由单事务完成清理与重写，
互换前后已登记资料（可印搭配集合）不丢失。

内存约束：合并与存法判定只持有「图版清单 + 语种清单 + 本批配对」，
进程额外内存随原图版数、原语种数与本批行数之和线性增长；
清单存法切到配对存法时，旧笛卡尔积由 SQL 侧 INSERT ... SELECT 一次写入，
本批零散新配对按块补写，全程不在进程内物化整份搭配
（大清单下物化会超出进程地址空间）。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from sys import intern
from typing import Iterable

from .db import run_with_immediate

# 流式写入 sparse_pairs 时每块行数：内存有界，同时保留 executemany 的批量优势
_INSERT_CHUNK_ROWS = 5000


@dataclass
class MergeResult:
    exhibit_no: str
    storage_mode: str
    switched_from: str | None
    rows_in_file: int
    unique_rows: int
    new_pairs: int
    total_pairs: int
    plates: list[str] = field(default_factory=list)
    languages: list[str] = field(default_factory=list)


def _dense_lists(
    conn: sqlite3.Connection, exhibit_no: str
) -> tuple[list[str], list[str]]:
    """清单存法的两份去重清单（内存随图版数 + 语种数线性增长）。"""
    plates = [
        r["plate_no"]
        for r in conn.execute(
            "SELECT plate_no FROM dense_plates WHERE exhibit_no = ? ORDER BY plate_no",
            (exhibit_no,),
        )
    ]
    languages = [
        r["language"]
        for r in conn.execute(
            "SELECT language FROM dense_languages WHERE exhibit_no = ? ORDER BY language",
            (exhibit_no,),
        )
    ]
    return plates, languages


def _insert_sparse_pairs(
    tx: sqlite3.Connection,
    exhibit_no: str,
    pairs: Iterable[tuple[str, str]],
    or_ignore: bool = False,
) -> int:
    """按块把配对写入 sparse_pairs，返回实际插入的行数。

    ``pairs`` 可以是生成器：整份笛卡尔积逐块流过，不在内存中物化。
    ``or_ignore=True`` 时与库内已有配对去重，返回值即净增配对数。
    """
    verb = "INSERT OR IGNORE INTO" if or_ignore else "INSERT INTO"
    sql = (
        f"{verb} sparse_pairs(exhibit_no, plate_no, language) VALUES (?, ?, ?)"
    )
    inserted = 0
    chunk: list[tuple[str, str, str]] = []
    for plate, language in pairs:
        chunk.append((exhibit_no, plate, language))
        if len(chunk) >= _INSERT_CHUNK_ROWS:
            before = tx.total_changes
            tx.executemany(sql, chunk)
            inserted += tx.total_changes - before
            chunk.clear()
    if chunk:
        before = tx.total_changes
        tx.executemany(sql, chunk)
        inserted += tx.total_changes - before
    return inserted


def _merge_with_dense(
    tx: sqlite3.Connection,
    exhibit_no: str,
    unique_incoming: list[tuple[str, str]],
) -> tuple[str, int, int, list[str], list[str]]:
    """当前为清单存法：只读两份清单做合并判定，必要时流式切换到配对存法。"""
    plates, languages = _dense_lists(tx, exhibit_no)
    plate_set = set(plates)
    lang_set = set(languages)

    # 本批落在已有笛卡尔积之外的配对（清单存法下只有这些才是净增）
    outside = [
        (p, lang)
        for p, lang in unique_incoming
        if p not in plate_set or lang not in lang_set
    ]
    new_pairs = len(outside)
    total_pairs = len(plate_set) * len(lang_set) + new_pairs

    new_plates = sorted({p for p, _ in unique_incoming} - plate_set)
    new_languages = sorted({lang for _, lang in unique_incoming} - lang_set)
    all_plates = sorted(plate_set | set(new_plates))
    all_languages = sorted(lang_set | set(new_languages))

    # 合并后仍构成完整笛卡尔积 ⟺ 本批把缺口区域全部填满：
    # 「旧图版 × 新语种」与「新图版 × 全部语种」两块都必须被本批覆盖。
    # outside 必然落在这两块之内，故比较计数即可，无需展开配对集合。
    required = len(plate_set) * len(new_languages) + len(new_plates) * len(
        all_languages
    )
    if new_pairs == required:
        # 仍为清单存法：只需把新图版/新语种追加进两份清单
        if new_plates:
            tx.executemany(
                "INSERT OR IGNORE INTO dense_plates(exhibit_no, plate_no) "
                "VALUES (?, ?)",
                [(exhibit_no, p) for p in new_plates],
            )
        if new_languages:
            tx.executemany(
                "INSERT OR IGNORE INTO dense_languages(exhibit_no, language) "
                "VALUES (?, ?)",
                [(exhibit_no, lang) for lang in new_languages],
            )
        # 防御性清理：维持「同一时刻只有一种存法的数据」这一不变式
        tx.execute("DELETE FROM sparse_pairs WHERE exhibit_no = ?", (exhibit_no,))
        return "dense", new_pairs, total_pairs, all_plates, all_languages

    # 清单出现缺项 -> 改存实际配对：旧笛卡尔积由 SQL 侧一次写入
    # （数据不出 SQLite，既不占进程内存也比逐行插入快得多），
    # 清单外新配对按块补写，保住原有的和本批新增的全部搭配。
    tx.execute(
        "INSERT INTO sparse_pairs(exhibit_no, plate_no, language) "
        "SELECT ?, p.plate_no, l.language "
        "FROM dense_plates p CROSS JOIN dense_languages l "
        "WHERE p.exhibit_no = ? AND l.exhibit_no = ?",
        (exhibit_no, exhibit_no, exhibit_no),
    )
    tx.execute("DELETE FROM dense_plates WHERE exhibit_no = ?", (exhibit_no,))
    tx.execute("DELETE FROM dense_languages WHERE exhibit_no = ?", (exhibit_no,))
    if outside:
        _insert_sparse_pairs(tx, exhibit_no, outside)
    return "sparse", new_pairs, total_pairs, all_plates, all_languages


def _merge_with_sparse(
    tx: sqlite3.Connection,
    exhibit_no: str,
    unique_incoming: list[tuple[str, str]],
) -> tuple[str, int, int, list[str], list[str]]:
    """当前为配对存法（或新展品）：本批去重并入，按计数判定是否补齐为清单。"""
    # 先并入本批：OR IGNORE 与库内去重，实际插入行数即净增配对数
    new_pairs = _insert_sparse_pairs(tx, exhibit_no, unique_incoming, or_ignore=True)
    total_pairs = tx.execute(
        "SELECT COUNT(*) FROM sparse_pairs WHERE exhibit_no = ?", (exhibit_no,)
    ).fetchone()[0]
    plates = [
        r["plate_no"]
        for r in tx.execute(
            "SELECT DISTINCT plate_no FROM sparse_pairs WHERE exhibit_no = ? "
            "ORDER BY plate_no",
            (exhibit_no,),
        )
    ]
    languages = [
        r["language"]
        for r in tx.execute(
            "SELECT DISTINCT language FROM sparse_pairs WHERE exhibit_no = ? "
            "ORDER BY language",
            (exhibit_no,),
        )
    ]

    # 已登记配对必然 ⊆ 图版集 × 语种集，计数相等即构成完整笛卡尔积
    # （空配对集不算完整，与原判定 bool(merged) 一致）
    if total_pairs and total_pairs == len(plates) * len(languages):
        # 缺项补齐 -> 改回清单存法，只存两份去重清单
        tx.execute("DELETE FROM sparse_pairs WHERE exhibit_no = ?", (exhibit_no,))
        tx.executemany(
            "INSERT INTO dense_plates(exhibit_no, plate_no) VALUES (?, ?)",
            [(exhibit_no, p) for p in plates],
        )
        tx.executemany(
            "INSERT INTO dense_languages(exhibit_no, language) VALUES (?, ?)",
            [(exhibit_no, lang) for lang in languages],
        )
        return "dense", new_pairs, total_pairs, plates, languages

    # 仍有缺项：保持配对存法（本批已在上面并入），防御性清理清单表
    tx.execute("DELETE FROM dense_plates WHERE exhibit_no = ?", (exhibit_no,))
    tx.execute("DELETE FROM dense_languages WHERE exhibit_no = ?", (exhibit_no,))
    return "sparse", new_pairs, total_pairs, plates, languages


def merge_and_store(
    conn: sqlite3.Connection,
    exhibit_no: str,
    incoming,
) -> MergeResult:
    """把本批配对去重后并入库内数据，并按合并结果重选存法。

    ``incoming`` 为 :class:`app.csvio.ParsedCsv`（也兼容纯 list 元组，
    便于单元测试）。整个读-改-写放在一个 BEGIN IMMEDIATE 事务里，
    并发批次串行化，任一失败（异常）都回滚，已提交的其它批次资料不受影响。

    内存占用随「原图版数 + 原语种数 + 本批行数」线性增长：
    清单存法不展开笛卡尔积，配对存法分块流式写入。
    """
    raw_data_rows = getattr(incoming, "raw_data_rows", None)
    pair_list = incoming.pairs if hasattr(incoming, "pairs") else list(incoming)
    rows_in_file = raw_data_rows if raw_data_rows is not None else len(pair_list)
    unique_incoming = sorted(set(pair_list))

    def work(tx: sqlite3.Connection) -> MergeResult:
        mode_row = tx.execute(
            "SELECT storage_mode FROM storage_state WHERE exhibit_no = ?", (exhibit_no,)
        ).fetchone()
        previous_mode = mode_row["storage_mode"] if mode_row else None

        if previous_mode == "dense":
            new_mode, new_pairs, total_pairs, plates, languages = _merge_with_dense(
                tx, exhibit_no, unique_incoming
            )
        else:
            new_mode, new_pairs, total_pairs, plates, languages = _merge_with_sparse(
                tx, exhibit_no, unique_incoming
            )
        # rows_in_file 为文件原始数据行数，unique_rows 为文件内去重后行数，
        # new_pairs 为相对已登记数据的净增，total_pairs 为合并后累计搭配数。

        tx.execute(
            "INSERT INTO storage_state(exhibit_no, storage_mode, updated_at) "
            "VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
            "ON CONFLICT(exhibit_no) DO UPDATE SET "
            "storage_mode = excluded.storage_mode, "
            "updated_at = excluded.updated_at",
            (exhibit_no, new_mode),
        )

        result = MergeResult(
            exhibit_no=exhibit_no,
            storage_mode=new_mode,
            switched_from=previous_mode if previous_mode != new_mode else None,
            rows_in_file=rows_in_file,
            unique_rows=len(unique_incoming),
            new_pairs=new_pairs,
            total_pairs=total_pairs,
            plates=plates,
            languages=languages,
        )
        tx.execute(
            "INSERT INTO batch_log(exhibit_no, rows_in_file, unique_rows, new_pairs, "
            "total_pairs, storage_mode, switched_from, status, message) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'committed', NULL)",
            (
                exhibit_no,
                result.rows_in_file,
                result.unique_rows,
                result.new_pairs,
                result.total_pairs,
                result.storage_mode,
                result.switched_from,
            ),
        )
        return result

    return run_with_immediate(conn, work)


def get_exhibit(conn: sqlite3.Connection, exhibit_no: str) -> dict | None:
    """网页 / API 查询：按展品和图版列出可印语种及当前存法。

    清单存法下不把笛卡尔积展开成 图版数 × 语种数 份拷贝：
    每个图版共享同一份语种清单对象，缺项核对用集合查找完成。
    """
    mode_row = conn.execute(
        "SELECT storage_mode, updated_at FROM storage_state WHERE exhibit_no = ?",
        (exhibit_no,),
    ).fetchone()
    if mode_row is None:
        return None

    mode = mode_row["storage_mode"]
    plates: list[str] = []
    languages: list[str] = []
    by_plate: dict[str, list[str]] = {}
    missing: list[list[str]] = []

    if mode == "dense":
        plates, languages = _dense_lists(conn, exhibit_no)
        # 清单存法下每个图版的可印语种都是同一份清单：共享同一 list 对象，
        # 内存随图版数 + 语种数线性增长，而非其乘积
        by_plate = dict.fromkeys(plates, languages)
        pair_count = len(plates) * len(languages)
    else:
        lang_set: set[str] = set()
        for r in conn.execute(
            "SELECT plate_no, language FROM sparse_pairs "
            "WHERE exhibit_no = ? ORDER BY plate_no, language",
            (exhibit_no,),
        ):
            # intern 让相同取值的图版号/语种共享字符串对象：
            # 百万级配对只保留去重后的字符串与引用，内存随清单大小线性增长
            plate = intern(r["plate_no"])
            language = intern(r["language"])
            by_plate.setdefault(plate, []).append(language)
            lang_set.add(language)
        plates = sorted(by_plate)
        languages = sorted(lang_set)
        pair_count = sum(len(langs) for langs in by_plate.values())
        # 只依据已登记数据核对缺项（集合查找，不构造配对元组大集合）
        for p in plates:
            have = set(by_plate[p])
            for lang in languages:
                if lang not in have:
                    missing.append([p, lang])

    batches = [
        dict(r)
        for r in conn.execute(
            "SELECT id, received_at, rows_in_file, unique_rows, new_pairs, "
            "total_pairs, storage_mode, switched_from, status, message "
            "FROM batch_log WHERE exhibit_no = ? ORDER BY id DESC",
            (exhibit_no,),
        )
    ]
    return {
        "exhibit_no": exhibit_no,
        "storage_mode": mode,
        "updated_at": mode_row["updated_at"],
        "plates": plates,
        "languages": languages,
        "by_plate": by_plate,
        "pair_count": pair_count,
        "is_complete": not missing,
        "missing_pairs": missing,
        "batches": batches,
    }


def list_exhibits(conn: sqlite3.Connection) -> list[dict]:
    return [
        dict(r)
        for r in conn.execute(
            "SELECT s.exhibit_no, s.storage_mode, s.updated_at, "
            "(SELECT COUNT(*) FROM dense_plates d WHERE d.exhibit_no = s.exhibit_no) "
            "AS plate_count_dense, "
            "(SELECT COUNT(*) FROM dense_languages d WHERE d.exhibit_no = s.exhibit_no) "
            "AS language_count_dense, "
            "(SELECT COUNT(DISTINCT p.plate_no) FROM sparse_pairs p "
            " WHERE p.exhibit_no = s.exhibit_no) AS plate_count_sparse, "
            "(SELECT COUNT(DISTINCT p.language) FROM sparse_pairs p "
            " WHERE p.exhibit_no = s.exhibit_no) AS language_count_sparse "
            "FROM storage_state s ORDER BY s.exhibit_no"
        )
    ]
