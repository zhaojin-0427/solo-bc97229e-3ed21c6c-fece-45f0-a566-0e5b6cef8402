#!/usr/bin/env bash
# 对运行中的平台做一遍完整演示(需先 docker compose up 或 ./run_local.sh)
# 用法: BASE=http://localhost:8000 ./demo.sh
set -euo pipefail
BASE="${BASE:-http://localhost:8000}"
HERE="$(cd "$(dirname "$0")" && pwd)"

upload() {
  curl -s -o /dev/null -w '%{redirect_url}' -F "file=@$1;filename=$(basename "$1")" "$BASE/upload"
}

wait_done() {
  local id="$1"
  while :; do
    st=$(curl -s "$BASE/api/batches/$id" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
    [ "$st" = processing ] || break
    sleep 0.5
  done
  echo "$st"
}

echo ">> 上传批次 A"
loc_a=$(upload "$HERE/samples/batch_a.csv"); id_a=${loc_a##*/}
echo ">> 上传重叠批次 B"
loc_b=$(upload "$HERE/samples/batch_b_overlap.csv"); id_b=${loc_b##*/}
echo ">> 上传坏批次(空样本号)"
loc_bad=$(upload "$HERE/samples/batch_bad_empty_sample.csv"); id_bad=${loc_bad##*/}

for pair in "A $id_a" "B $id_b" "BAD $id_bad"; do
  set -- $pair
  echo "批次 $1 最终状态: $(wait_done "$2")"
done

echo ">> 查询 S001 的正式映射:"
curl -s "$BASE/api/mappings?original_id=S001"; echo
echo ">> 坏批次下载(期望 409):"
curl -s -o /dev/null -w '%{http_code}\n' "$BASE/batches/$id_bad/download"
echo ">> 下载 A 批假名化文件到 /tmp:"
cd /tmp && curl -s -J -O "$BASE/batches/$id_a/download" && ls -1 /tmp/*_pseudonymized.csv | tail -1
