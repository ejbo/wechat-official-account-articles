"""
数据库配置 — 从项目根目录 .env 文件读取

优先级：.env 文件 > 系统环境变量 > 下方默认值
"""

import os
from dotenv import load_dotenv

# 加载项目根目录的 .env
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "wechat_articles"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

# 连接池配置
POOL_MIN_CONN = int(os.getenv("DB_POOL_MIN", "1"))
POOL_MAX_CONN = int(os.getenv("DB_POOL_MAX", "5"))

# 连接重试配置（断点重连）
RETRY_MAX_ATTEMPTS = int(os.getenv("DB_RETRY_MAX", "5"))
RETRY_DELAY_SECONDS = int(os.getenv("DB_RETRY_DELAY", "3"))


def get_dsn():
    """返回 PostgreSQL DSN 连接字符串"""
    c = DB_CONFIG
    return f"host={c['host']} port={c['port']} dbname={c['dbname']} user={c['user']} password={c['password']}"
