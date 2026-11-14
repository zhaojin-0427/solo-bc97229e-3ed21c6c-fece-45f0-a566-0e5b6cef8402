# 生物样本检测数据假名化交付平台

向合作方交付可**跨批关联**的检测数据，同时隐藏内部样本编号。
上传 CSV（`样本号`、`来源样本号`、`检测值`），系统将两个编号列中的每个原号
全局映射为随机 **UUID4**，输出可对外交付的假名化 CSV。

## 核心规则与保证

| 要求 | 实现方式 |
| --- | --- |
| 样本号非空、来源样本号可空 | 上传即校验，样本号空则整批失败，错误定位到行号 |
| 同一原号全局复用同一 UUID4 | 正式表 `id_mapping.original_id` 主键唯一；在途批次经 `mapping_reservation` 复用同一预留 |
| 空值保持为空 | 仅替换非空编号单元格，空来源号原样输出空串 |
| 不同原号不得同号 | `id_mapping.pseudonym` 与 `pseudonym_registry.pseudonym` 全局 UNIQUE，铸造 UUID 时冲突重试 |
| 其余内容、行序、重复行保留 | 仅改写两个编号列，逐行流式写回；BOM、额外列、CRLF、重复行均保留 |
| 后台并行转换，慢转换不阻塞其他批次/查询 | 固定线程池（默认 4 线程），慢转换 `sleep` 在线程中执行；DB 只持有毫秒级短事务 |
| 文件读写不占库写事务 | 先落盘上传文件、事务外写暂存 CSV 并 `fsync`，`os.replace` 原子就位后才提交发布事务 |
| 新映射、成功状态、完整 CSV 一起生效 | 文件先原子 rename 到正式路径，再在**单个事务**里提升预留映射 + 置 success |
| 重叠批次不交付不同编号 | 分配在 `BEGIN IMMEDIATE` 短事务内完成，后到批次复用先到批次的预留/正式编号 |
| 失败批次不新增可查映射、不删改他批映射 | 失败仅按 `batch_id` 清理本批预留与登记；正式表只由成功事务写入 |
| 重启保留成功映射及文件 | 数据全部在挂载卷 `/data`（SQLite + 输出文件） |
| 重启时未发布批次标失败并清残件 | 启动恢复：processing → failed，删预留/暂存/孤儿文件；success 但文件丢失则撤回该批独有映射 |
| 未成功不可下载 | 下载接口校验 `status='success'`，否则返回 409 |

## 数据表（SQLite，WAL 模式）

- `id_mapping` —— **正式映射**，只由成功发布事务写入，对外查询只看这张表。
- `pseudonym_registry` —— UUID 全局占用登记，预留阶段即写入，防止并发批次撞号。
- `mapping_reservation` —— 批次级预留，未发布前对外不可见，失败/重启按批次清除。
- `batch` —— 批次元数据与状态机：`processing → success | failed`。

## 快速启动（Docker Compose）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

演示流程：

1. 首页上传 `samples/batch_a.csv`，跳转到进度页自动轮询（演示有 2 秒慢转换）。
2. 再上传 `samples/batch_b_overlap.csv` —— 与 A 批重叠的 S001/S002/S003 得到**相同** UUID。
3. 上传 `samples/batch_bad_empty_sample.csv` —— 校验失败，无映射、不可下载。
4. “正式映射查询”页输入 `S001` 查看对外编号；成功批次在列表或进度页下载
   `*_pseudonymized.csv`。

数据持久化在命名卷 `pseudo-data`（容器内 `/data`），`docker compose restart`
或 `down/up` 后成功映射与文件保留，在途批次自动标记失败。

### 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PSEUDO_DATA_DIR` | `/data` | 数据根目录（uploads/outputs/SQLite） |
| `PSEUDO_WORKER_THREADS` | `4` | 并行转换线程数 |
| `PSEUDO_CONVERT_DELAY` | `2` | 演示用每批慢转换秒数，生产设 `0` |
| `PSEUDO_MAX_UPLOAD_MB` | `50` | 上传大小上限 |
| `PSEUDO_SECRET_KEY` | 内置开发值 | Flask 会话密钥，生产请覆盖 |

> 部署形态固定为 **单进程多线程**（gunicorn `-w 1 --threads 8`）。编号一致性由
> SQLite 事务与 UNIQUE 约束保证，不依赖进程内锁；如需水平扩展，应改用
> PostgreSQL/Redis 等带跨进程唯一约束的存储。

## 本地运行（无 Docker）

```bash
python3 -m venv --without-pip .venv && .venv/bin/python get-pip.py   # 仅在无 pip 的机器上
.venv/bin/pip install -r requirements.txt
./run_local.sh          # 或: PSEUDO_CONVERT_DELAY=0 ./run_local.sh
```

## HTTP 接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /upload` | multipart 上传 CSV，302 跳转到批次进度页 |
| `GET /batches/<id>` | 批次进度页面（1.5s 轮询） |
| `GET /api/batches/<id>` | 批次状态 JSON |
| `GET /mappings?original_id=S001` | 正式映射查询页 |
| `GET /api/mappings?original_id=S001` | 正式映射 JSON，未发布/不存在返回 404 |
| `GET /batches/<id>/download` | 下载成功文件；未成功返回 409 |

## 测试

```bash
.venv/bin/python -m pytest tests/ -q          # 19 个单元/集成测试
.venv/bin/python -I tests/smoke_e2e.py        # 对运行中服务的端到端冒烟（先启动服务）
```

测试覆盖：基础转换与保真、跨批全局复用/唯一性、并发重叠批次不发散、慢批次不阻塞、
失败隔离、预留不可见、重启恢复（含共享原号撤回语义）、完整 Web 流程。

## 目录结构

```
app/
  config.py     配置
  db.py         SQLite 连接/建表/短事务
  processor.py  校验、编号分配、暂存、原子发布、失败回滚、线程池
  recovery.py   重启恢复
  web.py        Flask 路由
  templates/    中文 Web 界面
samples/        演示 CSV（含坏文件样例）
tests/          pytest 套件 + E2E 冒烟脚本
Dockerfile, docker-compose.yml, wsgi.py, run_local.sh
```
