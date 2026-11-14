"""运行配置，全部来自环境变量，均有默认值。"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    data_dir: str
    db_path: str
    upload_dir: str
    output_dir: str
    # 并行转换线程数
    worker_threads: int
    # 演示用“慢转换”额外耗时(秒)，每个数据行最多加这么多的一部分
    convert_delay: float
    max_upload_mb: int
    # 单 worker 部署时仍然安全；作为额外的进程内分配串行点
    require_single_process: bool = True

    @staticmethod
    def from_env() -> "Config":
        data_dir = os.environ.get("PSEUDO_DATA_DIR", "/data")
        upload_dir = os.path.join(data_dir, "uploads")
        output_dir = os.path.join(data_dir, "outputs")
        db_path = os.environ.get("PSEUDO_DB_PATH", os.path.join(data_dir, "pseudo.db"))
        return Config(
            data_dir=data_dir,
            db_path=db_path,
            upload_dir=upload_dir,
            output_dir=output_dir,
            worker_threads=int(os.environ.get("PSEUDO_WORKER_THREADS", "4")),
            convert_delay=float(os.environ.get("PSEUDO_CONVERT_DELAY", "2")),
            max_upload_mb=int(os.environ.get("PSEUDO_MAX_UPLOAD_MB", "50")),
        )

    def ensure_dirs(self) -> None:
        for d in (self.data_dir, self.upload_dir, self.output_dir):
            os.makedirs(d, exist_ok=True)
