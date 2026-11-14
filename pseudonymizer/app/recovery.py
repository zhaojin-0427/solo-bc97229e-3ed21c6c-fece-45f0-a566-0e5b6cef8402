"""进程重启恢复。

- 重启前处于 processing 的批次一律标记 failed 并清除其预留/残件
  (其映射从未发布，不产生可查映射);
- 校验 success 批次的正式文件是否存在，缺失则降级为 failed 并撤回其映射——
  撤回只涉及本批次独有的原号，任何被其他成功批次共享的原号保持不动;
- 清理 output 目录里没有对应批次的暂存/孤儿文件。
"""
from __future__ import annotations

import logging
import os
import sqlite3

from . import db
from .config import Config
from .processor import output_path, utcnow

log = logging.getLogger("pseudonymizer")


def recover(cfg: Config) -> None:
    cfg.ensure_dirs()
    conn = db.get_conn(cfg)
    now = utcnow()

    rows = conn.execute("SELECT batch_id, status FROM batch").fetchall()
    known_ids = {r["batch_id"] for r in rows}

    for row in rows:
        batch_id, status = row["batch_id"], row["status"]
        final_path = output_path(cfg, batch_id)

        if status == "processing":
            _abort_inflight(conn, cfg, batch_id, "服务重启，未发布批次自动标记失败", now)
            log.warning("批次 %s 重启恢复: 未发布，标记失败", batch_id)
            continue

        if status == "success":
            if not os.path.exists(final_path):
                _withdraw_success(conn, cfg, batch_id, "重启检测到正式文件缺失", now)
                log.warning("批次 %s 重启恢复: 文件缺失，撤回并标记失败", batch_id)

    _sweep_orphan_files(cfg, known_ids)


def _abort_inflight(conn: sqlite3.Connection, cfg: Config, batch_id: str,
                    message: str, now: str) -> None:
    with db.transaction(conn):
        conn.execute("DELETE FROM mapping_reservation WHERE batch_id = ?", (batch_id,))
        conn.execute("DELETE FROM pseudonym_registry WHERE batch_id = ?", (batch_id,))
        conn.execute(
            "UPDATE batch SET status = 'failed', error = ?, updated_at = ? WHERE batch_id = ?",
            (message, now, batch_id),
        )
    _remove_residuals(cfg, batch_id)


def _withdraw_success(conn: sqlite3.Connection, cfg: Config, batch_id: str,
                      message: str, now: str) -> None:
    """正式文件丢失时，撤回仅由本批次发布的映射; 共享原号保留(其他批次仍有效)。"""
    with db.transaction(conn):
        # 只删除本批次创建、且没有被其他批次引用的原号映射
        conn.execute(
            "DELETE FROM id_mapping WHERE batch_id = ? AND original_id NOT IN "
            "(SELECT original_id FROM id_mapping WHERE batch_id != ?)",
            (batch_id, batch_id),
        )
        conn.execute(
            "UPDATE batch SET status = 'failed', error = ?, output_name = NULL, "
            "updated_at = ? WHERE batch_id = ?",
            (message, now, batch_id),
        )
    _remove_residuals(cfg, batch_id)


def _remove_residuals(cfg: Config, batch_id: str) -> None:
    for name in (f".staging.{batch_id}.csv", f"{batch_id}.csv"):
        path = os.path.join(cfg.output_dir, name)
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:  # pragma: no cover
            log.exception("恢复清理残件失败: %s", path)


def _sweep_orphan_files(cfg: Config, known_ids: set[str]) -> None:
    try:
        names = os.listdir(cfg.output_dir)
    except FileNotFoundError:
        return
    for name in names:
        path = os.path.join(cfg.output_dir, name)
        orphan = False
        if name.startswith(".staging.") and name.endswith(".csv"):
            orphan = True
        elif name.endswith(".csv"):
            orphan = name[:-4] not in known_ids
        if orphan:
            try:
                os.remove(path)
                log.warning("恢复清理孤儿文件: %s", name)
            except OSError:  # pragma: no cover
                log.exception("清理孤儿文件失败: %s", path)
