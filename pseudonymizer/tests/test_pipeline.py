"""端到端测试: 直接驱动处理流水线与 Flask 接口。"""
from __future__ import annotations

import csv
import io
import os
import time
import uuid

import pytest

from app import db, recovery
from app.config import Config
from app.processor import (
    BatchExecutor, output_path, parse_and_validate, process_batch,
)
from app.web import create_app


# ------------------------------------------------------------ 夹具

def make_config(tmp_path, delay: float = 0.0, workers: int = 4) -> Config:
    cfg = Config(
        data_dir=str(tmp_path / "data"),
        db_path=str(tmp_path / "data" / "pseudo.db"),
        upload_dir=str(tmp_path / "data" / "uploads"),
        output_dir=str(tmp_path / "data" / "outputs"),
        worker_threads=workers,
        convert_delay=delay,
        max_upload_mb=10,
    )
    cfg.ensure_dirs()
    db.init_db(cfg)
    return cfg


def seed_csv(cfg: Config, batch_id: str, rows_text: str, bom: bool = False) -> str:
    path = os.path.join(cfg.upload_dir, f"{batch_id}.csv")
    data = rows_text.encode("utf-8")
    if bom:
        data = b"\xef\xbb\xbf" + data
    with open(path, "wb") as f:
        f.write(data)
    return path


def register_batch(cfg: Config, batch_id: str, name: str | None = None) -> None:
    conn = db.get_conn(cfg)
    now = "2026-11-14T00:00:00+00:00"
    with db.transaction(conn):
        conn.execute(
            "INSERT INTO batch(batch_id, original_name, status, row_count, "
            "new_mappings, reuse_mappings, created_at, updated_at) "
            "VALUES (?, ?, 'processing', 0, 0, 0, ?, ?)",
            (batch_id, name or f"{batch_id}.csv", now, now),
        )


def run_and_wait(cfg: Config, batch_ids: list[str], timeout: float = 20) -> None:
    ex = BatchExecutor(cfg)
    for bid in batch_ids:
        ex.submit(bid)
    conn = db.get_conn(cfg)
    deadline = time.time() + timeout
    while time.time() < deadline:
        pending = conn.execute(
            "SELECT COUNT(*) c FROM batch WHERE status = 'processing'"
        ).fetchone()["c"]
        if pending == 0:
            ex.shutdown()
            return
        time.sleep(0.05)
    ex.shutdown()
    raise AssertionError("批次处理超时")


def status_of(cfg: Config, batch_id: str) -> str:
    return db.get_conn(cfg).execute(
        "SELECT status FROM batch WHERE batch_id = ?", (batch_id,)
    ).fetchone()["status"]


def read_output(cfg: Config, batch_id: str) -> list[list[str]]:
    with open(output_path(cfg, batch_id), encoding="utf-8-sig", newline="") as f:
        return list(csv.reader(f))


# ------------------------------------------------------------ 1. 基础转换

def test_basic_transform_preserves_rows_and_order(tmp_path):
    cfg = make_config(tmp_path)
    bid = uuid.uuid4().hex
    seed_csv(cfg, bid,
             "样本号,来源样本号,检测值\n"
             "S001,,3.65\n"
             "S002,S001,4.12\n"
             "S003,S001,3.98\n"
             "S004,,5.07\n"
             "S002,S001,4.12\n")  # 重复行
    register_batch(cfg, bid)
    process_batch(cfg, bid)

    assert status_of(cfg, bid) == "success"
    out = read_output(cfg, bid)
    assert out[0] == ["样本号", "来源样本号", "检测值"]
    assert len(out) == 6  # 表头 + 5 行(含重复行)
    # 空来源保持空
    assert out[1][1] == ""
    assert out[4][1] == ""
    # 同一原号在一行内/行间一致: S001 出现在第2行来源与第3行来源
    assert out[2][1] == out[3][1]
    # 重复行完全一致
    assert out[2] == out[5]
    # 检测值原样保留
    assert [r[2] for r in out[1:]] == ["3.65", "4.12", "3.98", "5.07", "4.12"]


def test_extra_columns_preserved(tmp_path):
    cfg = make_config(tmp_path)
    bid = uuid.uuid4().hex
    seed_csv(cfg, bid,
             "样本号,来源样本号,检测值,检测项目,送检机构\n"
             "S001,,1.1,ALT,北京中心\n"
             "S002,S001,2.2,AST,上海中心\n")
    register_batch(cfg, bid)
    process_batch(cfg, bid)
    out = read_output(cfg, bid)
    assert out[0] == ["样本号", "来源样本号", "检测值", "检测项目", "送检机构"]
    assert out[1][2:] == ["1.1", "ALT", "北京中心"]
    assert out[2][2:] == ["2.2", "AST", "上海中心"]
    # 非编号列均为原值，未被替换成 UUID
    assert "ALT" in out[1]


def test_bom_preserved_in_output(tmp_path):
    cfg = make_config(tmp_path)
    bid = uuid.uuid4().hex
    seed_csv(cfg, bid, "样本号,来源样本号,检测值\nS001,,1.0\n", bom=True)
    register_batch(cfg, bid)
    process_batch(cfg, bid)
    raw = open(output_path(cfg, bid), "rb").read()
    assert raw.startswith(b"\xef\xbb\xbf")


# ------------------------------------------------------------ 2. 全局唯一/复用

def test_global_reuse_and_uniqueness_across_batches(tmp_path):
    cfg = make_config(tmp_path)
    bid_a, bid_b = uuid.uuid4().hex, uuid.uuid4().hex
    seed_csv(cfg, bid_a,
             "样本号,来源样本号,检测值\nS001,,1\nS002,S001,2\nS003,,3\n")
    seed_csv(cfg, bid_b,
             "样本号,来源样本号,检测值\nS003,S002,4\nS005,,5\nS001,,6\n")
    register_batch(cfg, bid_a)
    register_batch(cfg, bid_b)
    process_batch(cfg, bid_a)
    process_batch(cfg, bid_b)

    assert status_of(cfg, bid_a) == "success"
    assert status_of(cfg, bid_b) == "success"
    conn = db.get_conn(cfg)
    rows = conn.execute("SELECT original_id, pseudonym FROM id_mapping").fetchall()
    originals = {r["original_id"] for r in rows}
    pseudos = [r["pseudonym"] for r in rows]
    assert originals == {"S001", "S002", "S003", "S005"}
    assert len(set(pseudos)) == len(pseudos)  # 不同原号不得同号
    # UUID4 合法性
    for p in pseudos:
        parsed = uuid.UUID(p)
        assert parsed.version == 4

    out_a = read_output(cfg, bid_a)[1:]
    out_b = read_output(cfg, bid_b)[1:]
    official = {r["original_id"]: r["pseudonym"] for r in rows}
    # 重叠原号跨批交付同一编号(以正式映射为基准核对输出)
    assert out_a[0][0] == official["S001"]            # A 批首行样本号 S001
    assert [r for r in out_b if r[2] == "6"][0][0] == official["S001"]
    assert [r for r in out_a if r[2] == "3"][0][0] == official["S003"]
    assert [r for r in out_b if r[2] == "4"][0][0] == official["S003"]
    # B 批中 S002 作为来源号，编号与正式映射的 S002 相同
    assert [r for r in out_b if r[2] == "4"][0][1] == official["S002"]

    stats = conn.execute("SELECT * FROM batch WHERE batch_id = ?", (bid_b,)).fetchone()
    assert stats["new_mappings"] == 1   # 仅 S005
    assert stats["reuse_mappings"] == 3  # S003/S002/S001


def test_concurrent_overlapping_batches_never_diverge(tmp_path):
    cfg = make_config(tmp_path, delay=0.4, workers=4)
    bids = []
    texts = [
        "样本号,来源样本号,检测值\nS001,,1\nS002,S001,2\n",
        "样本号,来源样本号,检测值\nS002,S001,3\nS003,,4\n",
        "样本号,来源样本号,检测值\nS003,S002,5\nS001,,6\n",
        "样本号,来源样本号,检测值\nS004,S001,7\nS005,S004,8\n",
    ]
    for t in texts:
        bid = uuid.uuid4().hex
        bids.append(bid)
        seed_csv(cfg, bid, t)
        register_batch(cfg, bid)
    run_and_wait(cfg, bids)

    conn = db.get_conn(cfg)
    assert all(status_of(cfg, b) == "success" for b in bids)
    rows = conn.execute("SELECT original_id, pseudonym FROM id_mapping").fetchall()
    pseudos = [r["pseudonym"] for r in rows]
    assert len(pseudos) == len(set(pseudos))
    official = {r["original_id"]: r["pseudonym"] for r in rows}

    # 每个输出文件里出现的编号必须与正式映射完全一致
    for bid in bids:
        for row in read_output(cfg, bid)[1:]:
            for cell in (row[0], row[1]):
                if cell:
                    assert cell in set(official.values())


def test_slow_batch_does_not_block_others(tmp_path):
    # 两个配置共用同一数据目录/数据库，但慢批次转换延迟 1.2s、快批次 0.2s
    base = make_config(tmp_path, delay=1.2, workers=2)
    fast_cfg = Config(
        data_dir=base.data_dir, db_path=base.db_path,
        upload_dir=base.upload_dir, output_dir=base.output_dir,
        worker_threads=2, convert_delay=0.2, max_upload_mb=10,
    )
    slow, fast = uuid.uuid4().hex, uuid.uuid4().hex
    seed_csv(base, slow, "样本号,来源样本号,检测值\nSLOW1,,1\nSLOW2,SLOW1,2\n")
    seed_csv(base, fast, "样本号,来源样本号,检测值\nFAST1,,9\n")
    register_batch(base, slow)
    register_batch(base, fast)

    ex_slow = BatchExecutor(base)
    ex_fast = BatchExecutor(fast_cfg)
    start = time.monotonic()
    ex_slow.submit(slow)
    time.sleep(0.05)
    ex_fast.submit(fast)

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if status_of(base, fast) != "processing":
            break
        time.sleep(0.02)
    elapsed = time.monotonic() - start
    # 快批次按自身 0.2s 延迟完成，且此时慢批次仍在转换中
    assert status_of(base, fast) == "success"
    assert status_of(base, slow) == "processing"
    assert elapsed < 0.9, f"快批次被慢批次阻塞，耗时 {elapsed:.2f}s"

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if status_of(base, slow) != "processing":
            break
        time.sleep(0.05)
    ex_slow.shutdown()
    ex_fast.shutdown()
    assert status_of(base, slow) == "success"


# ------------------------------------------------------------ 3. 失败隔离

def test_empty_sample_number_rejected(tmp_path):
    cfg = make_config(tmp_path)
    bid = uuid.uuid4().hex
    seed_csv(cfg, bid,
             "样本号,来源样本号,检测值\nS010,S099,1.0\n, ,4.0\n")
    register_batch(cfg, bid)
    process_batch(cfg, bid)
    assert status_of(cfg, bid) == "failed"
    conn = db.get_conn(cfg)
    assert conn.execute("SELECT COUNT(*) c FROM id_mapping").fetchone()["c"] == 0
    assert conn.execute("SELECT COUNT(*) c FROM mapping_reservation").fetchone()["c"] == 0
    assert conn.execute("SELECT COUNT(*) c FROM pseudonym_registry").fetchone()["c"] == 0
    assert not os.path.exists(output_path(cfg, bid))


def test_missing_header_rejected(tmp_path):
    cfg = make_config(tmp_path)
    bid = uuid.uuid4().hex
    seed_csv(cfg, bid, "编号,来源,检测值\nS1,,1\n")
    register_batch(cfg, bid)
    process_batch(cfg, bid)
    assert status_of(cfg, bid) == "failed"
    err = db.get_conn(cfg).execute(
        "SELECT error FROM batch WHERE batch_id = ?", (bid,)
    ).fetchone()["error"]
    assert "表头" in err


def test_failed_batch_does_not_touch_others(tmp_path):
    cfg = make_config(tmp_path)
    good, bad = uuid.uuid4().hex, uuid.uuid4().hex
    seed_csv(cfg, good, "样本号,来源样本号,检测值\nS001,,1\nS002,S001,2\n")
    seed_csv(cfg, bad, "样本号,来源样本号,检测值\nS003,,3\n,,4\n")
    register_batch(cfg, good)
    register_batch(cfg, bad)
    process_batch(cfg, good)
    before = {(r["original_id"], r["pseudonym"])
              for r in db.get_conn(cfg).execute(
                  "SELECT original_id, pseudonym FROM id_mapping").fetchall()}
    process_batch(cfg, bad)
    assert status_of(cfg, bad) == "failed"
    after = {(r["original_id"], r["pseudonym"])
             for r in db.get_conn(cfg).execute(
                 "SELECT original_id, pseudonym FROM id_mapping").fetchall()}
    assert before == after  # 成功批次映射未被删改
    assert os.path.exists(output_path(cfg, good))
    assert not os.path.exists(output_path(cfg, bad))


def test_reservation_invisible_until_published(tmp_path):
    # 处理中的预留映射不能通过正式映射查询看到
    cfg = make_config(tmp_path, delay=0.8)
    bid = uuid.uuid4().hex
    seed_csv(cfg, bid, "样本号,来源样本号,检测值\nSX1,,1\n")
    register_batch(cfg, bid)
    ex = BatchExecutor(cfg)
    ex.submit(bid)
    conn = db.get_conn(cfg)
    time.sleep(0.25)  # 此时分配已完成、批次尚未发布
    assert conn.execute(
        "SELECT 1 FROM mapping_reservation WHERE batch_id = ?", (bid,)
    ).fetchone() is not None
    assert conn.execute(
        "SELECT 1 FROM id_mapping WHERE original_id = 'SX1'"
    ).fetchone() is None
    deadline = time.time() + 10
    while time.time() < deadline:
        if status_of(cfg, bid) != "processing":
            break
        time.sleep(0.05)
    ex.shutdown()
    assert conn.execute(
        "SELECT 1 FROM id_mapping WHERE original_id = 'SX1'"
    ).fetchone() is not None


# ------------------------------------------------------------ 4. 重启恢复

def test_restart_marks_processing_failed_and_cleans(tmp_path):
    cfg = make_config(tmp_path)
    bid = uuid.uuid4().hex
    seed_csv(cfg, bid, "样本号,来源样本号,检测值\nSR1,,1\n")
    register_batch(cfg, bid)
    # 制造残件
    staging = os.path.join(cfg.output_dir, f".staging.{bid}.csv")
    with open(staging, "w") as f:
        f.write("junk")

    recovery.recover(cfg)
    conn = db.get_conn(cfg)
    assert status_of(cfg, bid) == "failed"
    assert conn.execute("SELECT COUNT(*) c FROM mapping_reservation").fetchone()["c"] == 0
    assert conn.execute("SELECT COUNT(*) c FROM pseudonym_registry").fetchone()["c"] == 0
    assert not os.path.exists(staging)


def test_restart_keeps_success_mappings_and_files(tmp_path):
    cfg = make_config(tmp_path)
    bid = uuid.uuid4().hex
    seed_csv(cfg, bid, "样本号,来源样本号,检测值\nSK1,,1\nSK2,SK1,2\n")
    register_batch(cfg, bid)
    process_batch(cfg, bid)
    mapping_before = {r["original_id"]: r["pseudonym"]
                      for r in db.get_conn(cfg).execute(
                          "SELECT original_id, pseudonym FROM id_mapping")}
    # 新连接模拟新进程启动
    db._local.conns = {}
    recovery.recover(cfg)
    mapping_after = {r["original_id"]: r["pseudonym"]
                     for r in db.get_conn(cfg).execute(
                         "SELECT original_id, pseudonym FROM id_mapping")}
    assert mapping_before == mapping_after
    assert status_of(cfg, bid) == "success"
    assert os.path.exists(output_path(cfg, bid))


def test_restart_withdraws_success_when_file_missing(tmp_path):
    cfg = make_config(tmp_path)
    bid = uuid.uuid4().hex
    seed_csv(cfg, bid, "样本号,来源样本号,检测值\nSM1,,1\n")
    register_batch(cfg, bid)
    process_batch(cfg, bid)
    os.remove(output_path(cfg, bid))
    recovery.recover(cfg)
    assert status_of(cfg, bid) == "failed"
    assert db.get_conn(cfg).execute(
        "SELECT COUNT(*) c FROM id_mapping WHERE original_id = 'SM1'"
    ).fetchone()["c"] == 0


def test_restart_shared_original_withdrawn_only_when_exclusive(tmp_path):
    # A、B 两个成功批次共享原号；B 文件丢失后，共享原号映射必须保留
    cfg = make_config(tmp_path)
    a, b = uuid.uuid4().hex, uuid.uuid4().hex
    seed_csv(cfg, a, "样本号,来源样本号,检测值\nSHARED,,1\nONLY_A,,2\n")
    seed_csv(cfg, b, "样本号,来源样本号,检测值\nSHARED,,3\nONLY_B,,4\n")
    register_batch(cfg, a)
    register_batch(cfg, b)
    process_batch(cfg, a)
    process_batch(cfg, b)
    os.remove(output_path(cfg, b))
    recovery.recover(cfg)
    conn = db.get_conn(cfg)
    assert status_of(cfg, b) == "failed"
    kept = {r["original_id"] for r in conn.execute(
        "SELECT original_id FROM id_mapping")}
    assert "SHARED" in kept
    assert "ONLY_A" in kept
    assert "ONLY_B" not in kept


# ------------------------------------------------------------ 5. Web 接口

@pytest.fixture()
def client(tmp_path):
    cfg = make_config(tmp_path, delay=0.3)
    app = create_app(cfg)
    app.config["PSEUDO_CFG"] = cfg
    with app.test_client() as c:
        yield c, cfg, app


def _upload(client, name, content: bytes):
    return client.post(
        "/upload",
        data={"file": (io.BytesIO(content), name)},
        content_type="multipart/form-data",
        follow_redirects=False,
    )


def _wait_status(cfg, bid, timeout=15):
    conn = db.get_conn(cfg)
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = conn.execute("SELECT status FROM batch WHERE batch_id = ?", (bid,)).fetchone()
        if st and st["status"] != "processing":
            return st["status"]
        time.sleep(0.05)
    raise AssertionError("timeout")


def test_web_upload_status_query_download_flow(client):
    c, cfg, _ = client
    resp = _upload(c, "batch_a.csv",
                   "样本号,来源样本号,检测值\nW1,,1\nW2,W1,2\n".encode())
    assert resp.status_code == 302
    bid = resp.headers["Location"].rstrip("/").split("/")[-1]

    api = c.get(f"/api/batches/{bid}").get_json()
    assert api["status"] in ("processing", "success")
    assert _wait_status(cfg, bid) == "success"
    api = c.get(f"/api/batches/{bid}").get_json()
    assert api["status"] == "success"

    # 正式映射查询
    q = c.get("/api/mappings?original_id=W1").get_json()
    assert q["found"] is True
    uuid.UUID(q["pseudonym"]).version == 4
    assert c.get("/api/mappings?original_id=NOPE").status_code == 404

    # 下载
    dl = c.get(f"/batches/{bid}/download")
    assert dl.status_code == 200
    assert "attachment" in dl.headers["Content-Disposition"]
    assert "batch_a_pseudonymized.csv" in dl.headers["Content-Disposition"]

    # 页面可渲染
    assert c.get("/").status_code == 200
    assert c.get(f"/batches/{bid}").status_code == 200
    assert c.get("/mappings?original_id=W1").status_code == 200


def test_web_failed_batch_not_downloadable(client):
    c, cfg, _ = client
    resp = _upload(c, "bad.csv",
                   "样本号,来源样本号,检测值\nS1,,1\n,,2\n".encode())
    bid = resp.headers["Location"].rstrip("/").split("/")[-1]
    assert _wait_status(cfg, bid) == "failed"
    dl = c.get(f"/batches/{bid}/download")
    assert dl.status_code == 409
    # 失败批次无映射可查
    assert c.get("/api/mappings?original_id=S1").status_code == 404


def test_web_rejects_non_csv(client):
    c, _, _ = client
    resp = _upload(c, "notes.txt", b"hello")
    assert resp.status_code == 302
    with c.session_transaction() as sess:
        flashes = [m for cat, m in sess["_flashes"]]
    assert any(".csv" in m for m in flashes)


def test_web_overlapping_uploads_consistent(client):
    c, cfg, _ = client
    r1 = _upload(c, "x1.csv", "样本号,来源样本号,检测值\nOV1,,1\nOV2,OV1,2\n".encode())
    r2 = _upload(c, "x2.csv", "样本号,来源样本号,检测值\nOV2,OV1,3\nOV3,,4\n".encode())
    b1 = r1.headers["Location"].rstrip("/").split("/")[-1]
    b2 = r2.headers["Location"].rstrip("/").split("/")[-1]
    assert _wait_status(cfg, b1) == "success"
    assert _wait_status(cfg, b2) == "success"

    def ids_in(bid):
        out = c.get(f"/batches/{bid}/download").data.decode("utf-8-sig")
        cells = set()
        parsed = list(csv.reader(io.StringIO(out)))
        for row in parsed[1:]:  # 跳过表头
            cells.update(x for x in row[:2] if x)
        return cells

    a, b = ids_in(b1), ids_in(b2)
    # 两批输出中的 UUID 集合在重叠原号上一致 => 交集非空且没有同原号异号
    assert a & b
    official = {r["pseudonym"] for r in db.get_conn(cfg).execute(
        "SELECT pseudonym FROM id_mapping")}
    assert a <= official and b <= official


def test_parse_validation_unit(tmp_path):
    cfg = make_config(tmp_path)
    p = seed_csv(cfg, "x", "样本号,来源样本号,检测值\nA,,1\n")
    has_bom, header, rows, si, ci = parse_and_validate(p)
    assert has_bom is False
    assert header == ["样本号", "来源样本号", "检测值"]
    assert (si, ci) == (0, 1)
    assert rows == [["A", "", "1"]]


def test_chinese_filename_kept_for_display_and_download(client):
    c, cfg, _ = client
    resp = _upload(c, "第一批检测.csv",
                   "样本号,来源样本号,检测值\nCN1,,1\n".encode())
    bid = resp.headers["Location"].rstrip("/").split("/")[-1]
    assert _wait_status(cfg, bid) == "success"
    row = db.get_conn(cfg).execute(
        "SELECT original_name, output_name FROM batch WHERE batch_id = ?", (bid,)
    ).fetchone()
    assert row["original_name"] == "第一批检测.csv"
    assert row["output_name"] == "第一批检测_pseudonymized.csv"
    dl = c.get(f"/batches/{bid}/download")
    assert dl.status_code == 200
    # 中文下载名经 RFC 5987 编码传输
    cd = dl.headers["Content-Disposition"].lower()
    assert "filename*=utf-8''" in cd
    assert "%e7%ac%ac" in cd  # “第”的百分号编码
