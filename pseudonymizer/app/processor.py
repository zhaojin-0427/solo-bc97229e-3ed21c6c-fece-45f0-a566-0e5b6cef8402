"""批次转换流水线。

设计要点
========
1. 所有文件读写(读上传 CSV、写暂存文件、os.replace 原子替换)都在数据库事务之外，
   数据库写事务只做短小的元数据/映射操作。
2. 编号分配在一个短事务内完成，借助 SQLite 的 UNIQUE 约束保证:
   - 同一原号全局复用同一个 UUID4(先查正式映射，再查其他批次的预留);
   - 不同原号绝不可能拿到同一个 UUID(pseudonym_registry 全局唯一登记);
   - 未发布映射只存在于 mapping_reservation，对外查询不可见。
3. 发布顺序: 先把完整 CSV 原子 rename 到正式路径，再在单个事务里把预留提升为
   正式映射并更新批次成功状态——三者一起对外生效; 任一步失败由重启恢复兜底。
4. 失败批次只删除本批次的预留/登记与残件，不触碰其他批次的任何数据。
"""
from __future__ import annotations

import csv
import io
import logging
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from . import db
from .config import Config

log = logging.getLogger("pseudonymizer")

SAMPLE_HEADER = "样本号"
SOURCE_HEADER = "来源样本号"
VALUE_HEADER = "检测值"
REQUIRED_HEADERS = (SAMPLE_HEADER, SOURCE_HEADER)


class BatchError(Exception):
    """可预期的批次处理错误(校验失败等)。"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- CSV 解析校验

def parse_and_validate(path: str) -> Tuple[bool, List[str], List[List[str]], int, int]:
    """读取并校验上传文件。

    返回 (是否带 BOM, 表头, 全部数据行(原样保留顺序与重复行), 样本号列下标, 来源列下标)。
    """
    try:
        raw = open(path, "rb").read()
    except OSError as exc:
        raise BatchError(f"上传文件无法读取: {exc}") from exc

    has_bom = raw.startswith(b"\xef\xbb\xbf")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise BatchError("文件必须为 UTF-8 编码的 CSV") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise BatchError("CSV 文件为空，缺少表头") from exc

    header_norm = [h.strip() for h in header]
    for name in REQUIRED_HEADERS:
        if name not in header_norm:
            raise BatchError(f"表头缺少必需列: {name}")
    sample_idx = header_norm.index(SAMPLE_HEADER)
    source_idx = header_norm.index(SOURCE_HEADER)

    rows: List[List[str]] = []
    for line_no, row in enumerate(reader, start=2):
        # csv.reader 会跳过空行；非空行列数必须与表头一致
        if len(row) != len(header):
            raise BatchError(f"第 {line_no} 行列数({len(row)})与表头({len(header)})不一致")
        if not row[sample_idx].strip():
            raise BatchError(f"第 {line_no} 行样本号为空，样本号不允许为空")
        rows.append(row)

    if not rows:
        raise BatchError("CSV 没有任何数据行")
    return has_bom, header, rows, sample_idx, source_idx


def collect_originals(rows: List[List[str]], sample_idx: int, source_idx: int) -> List[str]:
    """按首次出现顺序收集两列中的全部非空原号(去重、保序)。"""
    ordered: List[str] = []
    seen: set[str] = set()
    for row in rows:
        for idx in (sample_idx, source_idx):
            original = row[idx].strip()
            if original and original not in seen:
                seen.add(original)
                ordered.append(original)
    return ordered


# ---------------------------------------------------------------- 编号分配

def allocate_mappings(
    conn, batch_id: str, originals: List[str], now: str
) -> Tuple[Dict[str, str], int, int]:
    """在单个短事务内为批次分配全部编号。

    返回 (原号 -> 假名 映射, 新增数, 复用数)。
    """
    mapping: Dict[str, str] = {}
    new_count = 0
    reuse_count = 0

    with db.transaction(conn):
        for original in originals:
            # 1) 正式映射已存在 -> 全局复用
            row = conn.execute(
                "SELECT pseudonym FROM id_mapping WHERE original_id = ?", (original,)
            ).fetchone()
            if row is not None:
                pseudo = row["pseudonym"]
                reuse_count += 1
            else:
                # 2) 其他在途批次已预留同一原号 -> 复用其编号，保证重叠批次一致
                row = conn.execute(
                    "SELECT pseudonym FROM mapping_reservation WHERE original_id = ? LIMIT 1",
                    (original,),
                ).fetchone()
                if row is not None:
                    pseudo = row["pseudonym"]
                    reuse_count += 1
                else:
                    # 3) 铸造全新 UUID4，直到在正式表和全局登记表都不冲突
                    while True:
                        pseudo = str(uuid.uuid4())
                        clash = conn.execute(
                            "SELECT 1 FROM id_mapping WHERE pseudonym = ? "
                            "UNION ALL SELECT 1 FROM pseudonym_registry WHERE pseudonym = ? "
                            "LIMIT 1",
                            (pseudo, pseudo),
                        ).fetchone()
                        if clash is None:
                            break
                    conn.execute(
                        "INSERT INTO pseudonym_registry(pseudonym, original_id, batch_id) "
                        "VALUES (?, ?, ?)",
                        (pseudo, original, batch_id),
                    )
                    new_count += 1

            # 本批次自己的预留(发布时据此提升; 同原号同名允许多批次各留一行)
            conn.execute(
                "INSERT INTO mapping_reservation(batch_id, original_id, pseudonym) "
                "VALUES (?, ?, ?)",
                (batch_id, original, pseudo),
            )
            mapping[original] = pseudo

    return mapping, new_count, reuse_count


# ---------------------------------------------------------------- 输出文件

def write_staging_file(
    cfg: Config,
    batch_id: str,
    has_bom: bool,
    header: List[str],
    rows: List[List[str]],
    sample_idx: int,
    source_idx: int,
    mapping: Dict[str, str],
) -> str:
    """在事务之外写出完整暂存 CSV，返回暂存路径。"""
    staging = os.path.join(cfg.output_dir, f".staging.{batch_id}.csv")
    encoding = "utf-8-sig" if has_bom else "utf-8"
    with open(staging, "w", encoding=encoding, newline="") as f:
        writer = csv.writer(f, lineterminator="\r\n")
        writer.writerow(header)
        for row in rows:
            # 仅替换两个编号列; 空来源号保持为空; 其余列、行序、重复行原样保留
            sample_original = row[sample_idx].strip()
            row[sample_idx] = mapping[sample_original]
            source_original = row[source_idx].strip()
            row[source_idx] = mapping[source_original] if source_original else ""
            writer.writerow(row)
        f.flush()
        os.fsync(f.fileno())
    return staging


# ---------------------------------------------------------------- 发布 / 失败

def publish_batch(
    conn,
    batch_id: str,
    mapping: Dict[str, str],
    output_name: str,
    row_count: int,
    new_count: int,
    reuse_count: int,
    now: str,
) -> None:
    """文件已原子就位后，在单个事务内发布映射并置成功状态。"""
    with db.transaction(conn):
        for original, pseudo in mapping.items():
            conn.execute(
                "INSERT OR IGNORE INTO id_mapping(original_id, pseudonym, batch_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (original, pseudo, batch_id, now),
            )
            # 不可能发生的分歧防御: 全局唯一性约束保证同原号只会是同一名
            row = conn.execute(
                "SELECT pseudonym FROM id_mapping WHERE original_id = ?", (original,)
            ).fetchone()
            if row is None or row["pseudonym"] != pseudo:
                raise BatchError(f"编号冲突，原号 {original} 已映射到不同编号")

        conn.execute(
            "DELETE FROM mapping_reservation WHERE batch_id = ?", (batch_id,)
        )
        conn.execute(
            "DELETE FROM pseudonym_registry WHERE batch_id = ?", (batch_id,)
        )
        conn.execute(
            "UPDATE batch SET status = 'success', error = NULL, output_name = ?, "
            "row_count = ?, new_mappings = ?, reuse_mappings = ?, updated_at = ? "
            "WHERE batch_id = ?",
            (output_name, row_count, new_count, reuse_count, now, batch_id),
        )


def fail_batch(cfg: Config, conn, batch_id: str, message: str, now: str) -> None:
    """标记失败并清除本批次全部预留与残件; 不触碰其他批次。"""
    try:
        with db.transaction(conn):
            conn.execute(
                "DELETE FROM mapping_reservation WHERE batch_id = ?", (batch_id,)
            )
            conn.execute(
                "DELETE FROM pseudonym_registry WHERE batch_id = ?", (batch_id,)
            )
            conn.execute(
                "UPDATE batch SET status = 'failed', error = ?, updated_at = ? "
                "WHERE batch_id = ?",
                (message[:2000], now, batch_id),
            )
    except Exception:  # pragma: no cover - 兜底交给重启恢复
        log.exception("批次 %s 失败回滚事务异常，将由重启恢复处理", batch_id)

    for name in (f".staging.{batch_id}.csv", f"{batch_id}.csv"):
        path = os.path.join(cfg.output_dir, name)
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:  # pragma: no cover
            log.exception("清理残件失败: %s", path)


# ---------------------------------------------------------------- 流水线

def output_path(cfg: Config, batch_id: str) -> str:
    return os.path.join(cfg.output_dir, f"{batch_id}.csv")


def process_batch(cfg: Config, batch_id: str) -> None:
    """工作线程入口: 转换一个批次，任何异常都收敛为 failed 状态。"""
    conn = db.get_conn(cfg)
    now = utcnow()
    staging_path: str | None = None
    try:
        upload_path = os.path.join(cfg.upload_dir, f"{batch_id}.csv")
        has_bom, header, rows, sample_idx, source_idx = parse_and_validate(upload_path)

        originals = collect_originals(rows, sample_idx, source_idx)
        mapping, new_count, reuse_count = allocate_mappings(conn, batch_id, originals, now)

        # 演示用慢转换: 在线程池内并行，不阻塞其他批次，也不占用任何数据库事务
        if cfg.convert_delay > 0:
            time.sleep(cfg.convert_delay)

        staging_path = write_staging_file(
            cfg, batch_id, has_bom, header, rows, sample_idx, source_idx, mapping
        )

        final_path = output_path(cfg, batch_id)
        output_name = _derive_output_name(batch_id, conn)
        # 原子发布完整文件，随后立刻提交数据库事务 -> 一起对外生效
        os.replace(staging_path, final_path)
        staging_path = None

        publish_batch(
            conn, batch_id, mapping, output_name, len(rows),
            new_count, reuse_count, utcnow(),
        )
        log.info("批次 %s 发布成功: %d 行, 新增 %d, 复用 %d",
                 batch_id, len(rows), new_count, reuse_count)
    except Exception as exc:
        message = str(exc) or exc.__class__.__name__
        log.exception("批次 %s 处理失败: %s", batch_id, message)
        fail_batch(cfg, conn, batch_id, message, utcnow())
    finally:
        if staging_path and os.path.exists(staging_path):
            try:
                os.remove(staging_path)
            except OSError:  # pragma: no cover
                log.exception("清理暂存文件失败: %s", staging_path)


def _derive_output_name(batch_id: str, conn) -> str:
    row = conn.execute(
        "SELECT original_name FROM batch WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    original_name = row["original_name"] if row else f"{batch_id}.csv"
    base = original_name[:-4] if original_name.lower().endswith(".csv") else original_name
    return f"{base}_pseudonymized.csv"


class BatchExecutor:
    """固定大小线程池，批次之间并行转换; 串行点只在 SQLite 短事务上。"""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self._pool = ThreadPoolExecutor(
            max_workers=cfg.worker_threads,
            thread_name_prefix="batch-worker",
        )
        self._lock = threading.Lock()
        self._pending: set[str] = set()

    def submit(self, batch_id: str) -> bool:
        """投递批次。同一批次重复提交会被忽略(返回 False)，保证不会并行处理两次。"""
        with self._lock:
            if batch_id in self._pending:
                return False
            self._pending.add(batch_id)

        def _job() -> None:
            try:
                process_batch(self.cfg, batch_id)
            finally:
                with self._lock:
                    self._pending.discard(batch_id)

        self._pool.submit(_job)
        return True

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=False)
