"""假名化交付服务 (pseudonymizer)。

模块划分:
  config    - 环境变量配置
  db        - SQLite 连接 / 建表 / 短事务工具
  processor - 批次转换流水线(读 CSV、全局 UUID 分配、原子发布)
  recovery  - 进程重启后的恢复逻辑
  web       - Flask 路由与页面
"""
