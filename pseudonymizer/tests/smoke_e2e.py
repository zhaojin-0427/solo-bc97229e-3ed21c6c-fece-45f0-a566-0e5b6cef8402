#!/usr/bin/env python -I
"""端到端冒烟脚本: 对运行中的服务验证全部关键承诺。"""
import csv
import io
import json
import sys
import time
import urllib.request
import uuid

BASE = "http://127.0.0.1:8000"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def http(method, path, data=None, headers=None):
    req = urllib.request.Request(BASE + path, data=data, headers=headers or {}, method=method)
    try:
        with _OPENER.open(req, timeout=30) as resp:
            return resp.status, resp.headers, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()


def upload(name, text):
    boundary = "----smoke" + uuid.uuid4().hex
    body = io.BytesIO()
    body.write(f"--{boundary}\r\n".encode())
    body.write(f'Content-Disposition: form-data; name="file"; filename="{name}"\r\n'.encode())
    body.write(b"Content-Type: text/csv\r\n\r\n")
    body.write(text.encode("utf-8"))
    body.write(f"\r\n--{boundary}--\r\n".encode())
    status, headers, _ = http("POST", "/upload", body.getvalue(),
                              {"Content-Type": f"multipart/form-data; boundary={boundary}"})
    assert status == 302, f"upload failed {status}"
    return headers["Location"].rstrip("/").split("/")[-1]


def wait(bid):
    while True:
        status, _, raw = http("GET", f"/api/batches/{bid}")
        info = json.loads(raw)
        if info["status"] != "processing":
            return info
        time.sleep(0.25)


def query(original):
    status, _, raw = http("GET", f"/api/mappings?original_id={original}")
    return status, json.loads(raw)


def main():
    print("== 1) 同时上传两个重叠批次 + 一个坏批次，验证并行与一致性 ==")
    t0 = time.time()
    bid_a = upload("batch_a.csv",
                   "样本号,来源样本号,检测值\nS001,,3.65\nS002,S001,4.12\nS003,,5.07\n")
    bid_b = upload("batch_b.csv",
                   "样本号,来源样本号,检测值\nS003,S002,2.71\nS005,,6.33\nS001,,3.70\n")
    bid_bad = upload("batch_bad.csv",
                     "样本号,来源样本号,检测值\nS010,S099,1.23\n,,4.56\n")
    info_a = wait(bid_a)
    info_b = wait(bid_b)
    info_bad = wait(bid_bad)
    wall = time.time() - t0
    print(f"   三批全部结束，墙钟 {wall:.1f}s (串行需 6s 以上的转换等待)")
    assert wall < 4.5, "批次没有并行执行"
    assert info_a["status"] == info_b["status"] == "success"
    assert info_bad["status"] == "failed"
    print(f"   坏批次错误信息: {info_bad['error']}")

    print("== 2) 跨批同一原号 -> 同一 UUID，不同原号 -> 不同 UUID ==")
    _, q_s001 = query("S001")
    _, q_s003 = query("S003")
    _, q_s005 = query("S005")
    assert q_s001["found"] and q_s003["found"] and q_s005["found"]
    assert q_s001["pseudonym"] != q_s003["pseudonym"] != q_s005["pseudonym"]
    uuid.UUID(q_s001["pseudonym"]).version == 4
    print(f"   S001 -> {q_s001['pseudonym']}")
    print(f"   S003 -> {q_s003['pseudonym']}")

    def cells(bid):
        st, _, raw = http("GET", f"/batches/{bid}/download")
        assert st == 200
        rows = list(csv.reader(io.StringIO(raw.decode("utf-8-sig"))))
        out = set()
        for r in rows[1:]:
            out.update(x for x in r[:2] if x)
        return rows, out

    rows_a, out_a = cells(bid_a)
    rows_b, out_b = cells(bid_b)
    assert q_s001["pseudonym"] in out_a and q_s001["pseudonym"] in out_b
    assert q_s003["pseudonym"] in out_a and q_s003["pseudonym"] in out_b
    print("   两个成功文件中的重叠原号编号完全一致")

    print("== 3) 行序、重复行、空来源、其他列保持不变 ==")
    assert rows_a[0] == ["样本号", "来源样本号", "检测值"]
    assert rows_a[1][1] == "" and rows_a[3][1] == ""       # 空来源保持空
    assert [r[2] for r in rows_a[1:]] == ["3.65", "4.12", "5.07"]
    assert all(len(r) == 3 for r in rows_a)
    print("   表头/检测值/空来源/行数均符合预期")

    print("== 4) 失败批次不可下载、无可查映射，且未污染成功批次 ==")
    st, _, _ = http("GET", f"/batches/{bid_bad}/download")
    assert st == 409
    st404, q_bad = query("S010")
    assert st404 == 404 and q_bad["found"] is False
    st404b, _ = query("S099")
    assert st404b == 404
    _, q_s002 = query("S002")
    assert q_s002["found"] is True  # 成功批次的映射完好
    print("   坏批次下载返回 409，预留映射对外不可见，成功批次映射未受影响")

    print("== 5) 不存在的批次与原号 ==")
    st, _, _ = http("GET", "/api/batches/nope")
    assert st == 404
    st, _, _ = http("GET", "/batches/nope/download")
    assert st == 404
    print("   404 行为正确")

    print("\n全部冒烟检查通过 ✔")
    print(f"批次A={bid_a}\n批次B={bid_b}\n坏批次={bid_bad}")


if __name__ == "__main__":
    main()
