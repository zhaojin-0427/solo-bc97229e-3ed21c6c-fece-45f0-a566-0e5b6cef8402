#!/usr/bin/env bash
# 本地(非 Docker)演示启动脚本
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  echo "请先创建虚拟环境并安装依赖: pip install -r requirements.txt"
  exit 1
fi

export PSEUDO_DATA_DIR="${PSEUDO_DATA_DIR:-$(pwd)/data}"
export PSEUDO_CONVERT_DELAY="${PSEUDO_CONVERT_DELAY:-2}"
mkdir -p "$PSEUDO_DATA_DIR/uploads" "$PSEUDO_DATA_DIR/outputs"

echo "数据目录: $PSEUDO_DATA_DIR"
echo "打开 http://127.0.0.1:8000"
exec .venv/bin/gunicorn -w 1 --threads 8 -b 0.0.0.0:8000 --timeout 180 wsgi:app
