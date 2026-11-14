"""Flask Web 入口: 上传、进度、正式映射查询、成功文件下载。"""
from __future__ import annotations

import logging
import os
import uuid

from flask import (
    Flask, abort, flash, jsonify, redirect, render_template, request,
    send_from_directory, url_for,
)

from . import db, recovery
from .config import Config
from .processor import BatchExecutor, output_path, utcnow

log = logging.getLogger("pseudonymizer")

STATUS_LABELS = {
    "processing": "转换中",
    "success": "成功",
    "failed": "失败",
}


def create_app(config: Config | None = None) -> Flask:
    cfg = config or Config.from_env()
    cfg.ensure_dirs()
    db.init_db(cfg)
    recovery.recover(cfg)

    app = Flask(__name__)
    app.config["PSEUDO_CFG"] = cfg
    app.secret_key = os.environ.get("PSEUDO_SECRET_KEY", "dev-only-pseudonymizer-key")
    app.config["MAX_CONTENT_LENGTH"] = cfg.max_upload_mb * 1024 * 1024

    executor = BatchExecutor(cfg)
    app.extensions["batch_executor"] = executor

    # ---------------------------------------------------------------- 首页/批次列表
    @app.get("/")
    def index():
        conn = db.get_conn(cfg)
        batches = conn.execute(
            "SELECT * FROM batch ORDER BY created_at DESC, batch_id DESC"
        ).fetchall()
        return render_template("index.html", batches=batches,
                               status_labels=STATUS_LABELS)

    # ---------------------------------------------------------------- 上传
    @app.post("/upload")
    def upload():
        file = request.files.get("file")
        if file is None or not file.filename:
            flash("请选择要上传的 CSV 文件", "error")
            return redirect(url_for("index"))

        filename = file.filename
        if not filename.lower().endswith(".csv"):
            flash("仅支持 .csv 文件", "error")
            return redirect(url_for("index"))

        # 文件始终以 batch_id 落盘，原始文件名仅用于展示与下载命名；
        # 用 basename 剥离路径分量防穿越，保留中文文件名
        safe_name = os.path.basename(filename)
        if not safe_name or safe_name in (".", ".."):
            safe_name = f"{uuid.uuid4().hex}.csv"
        if not safe_name.lower().endswith(".csv"):
            safe_name += ".csv"

        batch_id = uuid.uuid4().hex
        target = os.path.join(cfg.upload_dir, f"{batch_id}.csv")
        # 先落盘(不占用任何数据库事务)，再登记批次并投递线程池
        file.save(target)
        size = os.path.getsize(target)
        if size == 0:
            os.remove(target)
            flash("上传文件为空", "error")
            return redirect(url_for("index"))

        now = utcnow()
        conn = db.get_conn(cfg)
        with db.transaction(conn):
            conn.execute(
                "INSERT INTO batch(batch_id, original_name, status, row_count, "
                "new_mappings, reuse_mappings, created_at, updated_at) "
                "VALUES (?, ?, 'processing', 0, 0, 0, ?, ?)",
                (batch_id, safe_name, now, now),
            )
        executor.submit(batch_id)
        flash(f"批次已受理: {batch_id}", "ok")
        return redirect(url_for("progress_page", batch_id=batch_id))

    # ---------------------------------------------------------------- 进度
    @app.get("/batches/<batch_id>")
    def progress_page(batch_id: str):
        conn = db.get_conn(cfg)
        batch = conn.execute(
            "SELECT * FROM batch WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if batch is None:
            abort(404)
        return render_template("progress.html", batch=batch,
                               status_labels=STATUS_LABELS)

    @app.get("/api/batches/<batch_id>")
    def batch_status(batch_id: str):
        conn = db.get_conn(cfg)
        batch = conn.execute(
            "SELECT * FROM batch WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if batch is None:
            return jsonify({"error": "批次不存在"}), 404
        return jsonify({
            "batch_id": batch["batch_id"],
            "status": batch["status"],
            "status_text": STATUS_LABELS[batch["status"]],
            "error": batch["error"],
            "row_count": batch["row_count"],
            "new_mappings": batch["new_mappings"],
            "reuse_mappings": batch["reuse_mappings"],
            "output_name": batch["output_name"],
            "download_url": url_for("download", batch_id=batch["batch_id"]),
        })

    # ---------------------------------------------------------------- 正式映射查询
    @app.get("/mappings")
    def mappings_page():
        original = request.args.get("original_id", "").strip()
        result = None
        if original:
            conn = db.get_conn(cfg)
            result = conn.execute(
                "SELECT * FROM id_mapping WHERE original_id = ?", (original,)
            ).fetchone()
        return render_template("mappings.html", original=original, result=result)

    @app.get("/api/mappings")
    def mapping_api():
        original = request.args.get("original_id", "").strip()
        if not original:
            return jsonify({"error": "缺少 original_id 参数"}), 400
        conn = db.get_conn(cfg)
        row = conn.execute(
            "SELECT * FROM id_mapping WHERE original_id = ?", (original,)
        ).fetchone()
        if row is None:
            return jsonify({
                "original_id": original,
                "found": False,
                "pseudonym": None,
                "message": "未找到已发布的正式映射(批次可能尚未成功)",
            }), 404
        return jsonify({
            "original_id": row["original_id"],
            "found": True,
            "pseudonym": row["pseudonym"],
            "batch_id": row["batch_id"],
            "created_at": row["created_at"],
        })

    # ---------------------------------------------------------------- 下载
    @app.get("/batches/<batch_id>/download")
    def download(batch_id: str):
        conn = db.get_conn(cfg)
        batch = conn.execute(
            "SELECT * FROM batch WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if batch is None:
            abort(404)
        if batch["status"] != "success" or not batch["output_name"]:
            # 未成功批次绝不允许下载
            abort(409, description="批次尚未成功，文件不可下载")
        download_name = batch["output_name"]
        return send_from_directory(
            cfg.output_dir, f"{batch_id}.csv",
            as_attachment=True, download_name=download_name,
        )

    @app.errorhandler(413)
    def too_large(_exc):
        flash(f"文件超过大小限制({cfg.max_upload_mb} MB)", "error")
        return redirect(url_for("index"))

    @app.errorhandler(409)
    def conflict(exc):
        return render_template("error.html", message=exc.description), 409

    @app.errorhandler(404)
    def not_found(_exc):
        return render_template("error.html", message="资源不存在"), 404

    return app
