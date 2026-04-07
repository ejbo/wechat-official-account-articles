"""
PostgreSQL 数据库层 — 建表、连接管理、CRUD 操作

表设计兼容 Django ORM 命名规范（snake_case, id 主键），
方便以后直接在 Django 中用 inspectdb 生成 models.py。
"""

import logging
import re
import time
from datetime import datetime

import psycopg2
import psycopg2.extras
import psycopg2.pool
from bs4 import BeautifulSoup

from db_config import DB_CONFIG, POOL_MIN_CONN, POOL_MAX_CONN, RETRY_MAX_ATTEMPTS, RETRY_DELAY_SECONDS

log = logging.getLogger("database")


def html_to_text(html):
    """
    将微信文章 HTML 提炼为干净的纯文本，供模型训练使用。
    去除所有标签、多余空白、脚本/样式，保留段落换行。
    """
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # 删除 script / style
    for tag in soup(["script", "style"]):
        tag.decompose()
    # 在块级元素前后插入换行，保留段落结构
    for br in soup.find_all("br"):
        br.replace_with("\n")
    for tag in soup.find_all(["p", "div", "section", "h1", "h2", "h3", "h4", "h5", "h6", "li", "blockquote"]):
        tag.insert_before("\n")
        tag.insert_after("\n")
    text = soup.get_text()
    # 清理：合并连续空白行，去掉行首尾空格
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]  # 去空行
    return "\n".join(lines)

# ==================== 建表 SQL ====================

SCHEMA_SQL = """
-- 数据来源（公众号 / 专栏）
CREATE TABLE IF NOT EXISTS sources (
    id              SERIAL PRIMARY KEY,
    slug            VARCHAR(64)  NOT NULL UNIQUE,       -- 专栏标识，如 pJMG8ZXFLd
    name            VARCHAR(255) NOT NULL DEFAULT '',    -- 公众号名称，如 "机器之心"
    platform        VARCHAR(64)  NOT NULL DEFAULT 'wechat',  -- 平台: wechat / zhihu / ...
    description     TEXT         NOT NULL DEFAULT '',
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- 文章主表
CREATE TABLE IF NOT EXISTS articles (
    id              SERIAL PRIMARY KEY,
    source_id       INTEGER      NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    article_hash    VARCHAR(32)  NOT NULL,               -- md5(original_url)[:10] 兼容现有 article_id
    title           VARCHAR(512) NOT NULL DEFAULT '',
    author          VARCHAR(255) NOT NULL DEFAULT '',
    publish_time    TIMESTAMP,                            -- 文章发布时间
    original_url    TEXT         NOT NULL,                -- 微信原文链接
    cover_url       TEXT         NOT NULL DEFAULT '',     -- 远程封面 URL
    content_html    TEXT         NOT NULL DEFAULT '',     -- 正文 HTML（图片链接已替换为本地）
    content_text    TEXT         NOT NULL DEFAULT '',     -- 纯文本正文（供模型训练）
    fetched_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_article_url UNIQUE (original_url)
);
CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source_id);
CREATE INDEX IF NOT EXISTS idx_articles_publish_time ON articles(publish_time);
CREATE INDEX IF NOT EXISTS idx_articles_hash ON articles(article_hash);

-- 图片表 — 二进制存储在 BYTEA 中
CREATE TABLE IF NOT EXISTS images (
    id              SERIAL PRIMARY KEY,
    article_id      INTEGER      NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    image_type      VARCHAR(16)  NOT NULL DEFAULT 'content',  -- 'cover' | 'content'
    image_index     INTEGER      NOT NULL DEFAULT 0,          -- 正文图片序号，封面为 0
    original_url    TEXT         NOT NULL DEFAULT '',          -- 远程原始 URL
    filename        VARCHAR(255) NOT NULL DEFAULT '',          -- 如 cover.jpg, 1.png
    mime_type       VARCHAR(64)  NOT NULL DEFAULT 'image/jpeg',
    data            BYTEA,                                     -- 图片二进制数据
    file_size       INTEGER      NOT NULL DEFAULT 0,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_images_article ON images(article_id);
CREATE INDEX IF NOT EXISTS idx_images_type ON images(image_type);

-- 爬取队列（断点续爬）
CREATE TABLE IF NOT EXISTS scrape_queue (
    id              SERIAL PRIMARY KEY,
    source_id       INTEGER      NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    original_url    TEXT         NOT NULL,
    title           VARCHAR(512) NOT NULL DEFAULT '',
    author          VARCHAR(255) NOT NULL DEFAULT '',
    publish_time    VARCHAR(32)  NOT NULL DEFAULT '',
    cover_url       TEXT         NOT NULL DEFAULT '',
    raw_meta        JSONB,                                -- API 返回的原始 JSON，保留完整信息
    status          VARCHAR(16)  NOT NULL DEFAULT 'pending',  -- pending | processing | done | failed
    retry_count     INTEGER      NOT NULL DEFAULT 0,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_queue_url UNIQUE (source_id, original_url)
);
CREATE INDEX IF NOT EXISTS idx_queue_status ON scrape_queue(status);
CREATE INDEX IF NOT EXISTS idx_queue_source ON scrape_queue(source_id);

-- 翻页进度（断点续接 fetch）
CREATE TABLE IF NOT EXISTS scrape_progress (
    id                  SERIAL PRIMARY KEY,
    source_id           INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    last_completed_page INTEGER NOT NULL DEFAULT 0,
    next_page           INTEGER NOT NULL DEFAULT 1,
    total_articles      INTEGER NOT NULL DEFAULT 0,
    stop_reason         VARCHAR(64) NOT NULL DEFAULT '',
    page_details        JSONB,           -- 各页详情
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_progress_source UNIQUE (source_id)
);
"""

# 对已存在的旧表做结构迁移（幂等）
MIGRATION_SQL = """
DO $$
BEGIN
    -- 1. articles: 删除 is_first（如果存在）
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'articles' AND column_name = 'is_first'
    ) THEN
        ALTER TABLE articles DROP COLUMN is_first;
    END IF;

    -- 2. articles: publish_time VARCHAR → TIMESTAMP（如果还是字符串类型）
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'articles' AND column_name = 'publish_time'
          AND data_type IN ('character varying', 'text')
    ) THEN
        ALTER TABLE articles ALTER COLUMN publish_time DROP NOT NULL;
        ALTER TABLE articles ALTER COLUMN publish_time DROP DEFAULT;
        ALTER TABLE articles ALTER COLUMN publish_time TYPE TIMESTAMP
            USING CASE
                WHEN publish_time ~ '^[0-9]{14}$'
                    THEN to_timestamp(publish_time, 'YYYYMMDDHH24MISS')
                ELSE NULL
            END;
    END IF;

    -- 3. scrape_queue: 删除 is_first（如果存在）
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'scrape_queue' AND column_name = 'is_first'
    ) THEN
        ALTER TABLE scrape_queue DROP COLUMN is_first;
    END IF;
END $$;
"""


# ==================== 连接管理（断点重连） ====================

class Database:
    """带自动重连和连接池的数据库管理器"""

    def __init__(self):
        self._pool = None

    def connect(self):
        """建立连接池，支持重试"""
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    POOL_MIN_CONN, POOL_MAX_CONN, **DB_CONFIG
                )
                log.info("数据库连接池已建立")
                return
            except psycopg2.OperationalError as e:
                log.warning(f"数据库连接失败 (第 {attempt}/{RETRY_MAX_ATTEMPTS} 次): {e}")
                if attempt < RETRY_MAX_ATTEMPTS:
                    time.sleep(RETRY_DELAY_SECONDS)
        raise ConnectionError(f"无法连接数据库，已重试 {RETRY_MAX_ATTEMPTS} 次")

    def get_conn(self):
        """从池中获取连接，如果池断开则重连"""
        if self._pool is None or self._pool.closed:
            self.connect()
        try:
            conn = self._pool.getconn()
            conn.autocommit = False
            return conn
        except (psycopg2.pool.PoolError, psycopg2.OperationalError):
            log.warning("连接池异常，尝试重建...")
            self.connect()
            conn = self._pool.getconn()
            conn.autocommit = False
            return conn

    def put_conn(self, conn):
        if self._pool and not self._pool.closed:
            self._pool.putconn(conn)

    def close(self):
        if self._pool and not self._pool.closed:
            self._pool.closeall()
            log.info("数据库连接池已关闭")

    def execute_with_retry(self, func):
        """执行数据库操作，自动重试断连"""
        last_err = None
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            conn = None
            try:
                conn = self.get_conn()
                result = func(conn)
                conn.commit()
                return result
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_err = e
                log.warning(f"数据库操作失败 (第 {attempt} 次): {e}")
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    try:
                        self.put_conn(conn)
                    except Exception:
                        pass
                    conn = None
                if attempt < RETRY_MAX_ATTEMPTS:
                    time.sleep(RETRY_DELAY_SECONDS)
                    # 尝试重建池
                    try:
                        self.connect()
                    except Exception:
                        pass
            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                raise
            finally:
                if conn:
                    self.put_conn(conn)
        raise last_err

    def init_schema(self):
        """创建所有表并执行结构迁移（幂等）"""
        def _do(conn):
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
                cur.execute(MIGRATION_SQL)
            log.info("数据库表结构已就绪")
        self.execute_with_retry(_do)


# ==================== 工具函数 ====================

def parse_publish_time(raw):
    """
    将 API 返回的发布时间字符串解析为 datetime。
    支持 '20260324093032'（14位数字）及 ISO 格式。
    无法解析时返回 None。
    """
    if not raw:
        return None
    raw = str(raw).strip()
    if len(raw) == 14 and raw.isdigit():
        try:
            return datetime.strptime(raw, "%Y%m%d%H%M%S")
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        pass
    return None


# ==================== 数据操作 ====================

# --- sources ---

def get_or_create_source(db, slug, name="", platform="wechat"):
    def _do(conn):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM sources WHERE slug = %s", (slug,))
            row = cur.fetchone()
            if row:
                # 名字有变化时同步更新
                if name and row["name"] != name:
                    cur.execute(
                        "UPDATE sources SET name = %s, updated_at = NOW() WHERE slug = %s",
                        (name, slug)
                    )
                    row = dict(row)
                    row["name"] = name
                return dict(row)
            cur.execute(
                "INSERT INTO sources (slug, name, platform) VALUES (%s, %s, %s) RETURNING *",
                (slug, name, platform)
            )
            return dict(cur.fetchone())
    return db.execute_with_retry(_do)


# --- articles ---

def article_exists(db, original_url):
    def _do(conn):
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM articles WHERE original_url = %s", (original_url,))
            return cur.fetchone() is not None
    return db.execute_with_retry(_do)


def get_existing_urls(db, source_id):
    def _do(conn):
        with conn.cursor() as cur:
            cur.execute("SELECT original_url FROM articles WHERE source_id = %s", (source_id,))
            return {row[0] for row in cur.fetchall()}
    return db.execute_with_retry(_do)


def insert_article(db, source_id, article_hash, title, author, publish_time,
                   original_url, cover_url, content_html, fetched_at):
    content_text = html_to_text(content_html)
    parsed_time = parse_publish_time(publish_time)

    def _do(conn):
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO articles
                    (source_id, article_hash, title, author, publish_time,
                     original_url, cover_url, content_html, content_text, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (original_url) DO NOTHING
                RETURNING id
            """, (source_id, article_hash, title, author, parsed_time,
                  original_url, cover_url, content_html, content_text, fetched_at))
            row = cur.fetchone()
            return row[0] if row else None
    return db.execute_with_retry(_do)


def get_articles(db, source_id=None, limit=100, offset=0):
    def _do(conn):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if source_id:
                cur.execute("""
                    SELECT id, source_id, article_hash, title, author, publish_time,
                           original_url, cover_url, content_html, fetched_at
                    FROM articles WHERE source_id = %s
                    ORDER BY fetched_at DESC LIMIT %s OFFSET %s
                """, (source_id, limit, offset))
            else:
                cur.execute("""
                    SELECT id, source_id, article_hash, title, author, publish_time,
                           original_url, cover_url, content_html, fetched_at
                    FROM articles
                    ORDER BY fetched_at DESC LIMIT %s OFFSET %s
                """, (limit, offset))
            return [dict(r) for r in cur.fetchall()]
    return db.execute_with_retry(_do)


def count_articles(db, source_id=None):
    def _do(conn):
        with conn.cursor() as cur:
            if source_id:
                cur.execute("SELECT COUNT(*) FROM articles WHERE source_id = %s", (source_id,))
            else:
                cur.execute("SELECT COUNT(*) FROM articles")
            return cur.fetchone()[0]
    return db.execute_with_retry(_do)


# --- images ---

def insert_image(db, article_id, image_type, image_index, original_url,
                 filename, mime_type, data):
    def _do(conn):
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO images
                    (article_id, image_type, image_index, original_url,
                     filename, mime_type, data, file_size)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (article_id, image_type, image_index, original_url,
                  filename, mime_type, psycopg2.Binary(data) if data else None,
                  len(data) if data else 0))
            return cur.fetchone()[0]
    return db.execute_with_retry(_do)


def get_image(db, image_id):
    def _do(conn):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM images WHERE id = %s", (image_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    return db.execute_with_retry(_do)


def get_image_by_article_and_filename(db, article_id, filename):
    def _do(conn):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM images WHERE article_id = %s AND filename = %s",
                (article_id, filename)
            )
            row = cur.fetchone()
            return dict(row) if row else None
    return db.execute_with_retry(_do)


def get_images_by_article(db, article_id):
    def _do(conn):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, image_type, image_index, filename, mime_type, file_size "
                "FROM images WHERE article_id = %s ORDER BY image_type, image_index",
                (article_id,)
            )
            return [dict(r) for r in cur.fetchall()]
    return db.execute_with_retry(_do)


# --- scrape_queue ---

def enqueue_articles(db, source_id, items):
    """批量入队，跳过已存在的 URL。返回新增数量。"""
    if not items:
        return 0

    def _do(conn):
        added = 0
        with conn.cursor() as cur:
            for item in items:
                try:
                    cur.execute("""
                        INSERT INTO scrape_queue
                            (source_id, original_url, title, author, publish_time,
                             cover_url, raw_meta)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_id, original_url) DO NOTHING
                    """, (
                        source_id,
                        item.get("original_url", ""),
                        item.get("name") or item.get("title", ""),
                        item.get("author", ""),
                        item.get("publish_time", ""),
                        item.get("image", ""),
                        psycopg2.extras.Json(item),
                    ))
                    if cur.rowcount > 0:
                        added += 1
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()
        return added
    return db.execute_with_retry(_do)


def get_pending_queue(db, source_id, limit=100):
    def _do(conn):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM scrape_queue
                WHERE source_id = %s AND status = 'pending'
                ORDER BY id ASC LIMIT %s
            """, (source_id, limit))
            return [dict(r) for r in cur.fetchall()]
    return db.execute_with_retry(_do)


def count_queue(db, source_id, status="pending"):
    def _do(conn):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM scrape_queue WHERE source_id = %s AND status = %s",
                (source_id, status)
            )
            return cur.fetchone()[0]
    return db.execute_with_retry(_do)


def update_queue_status(db, queue_id, status):
    def _do(conn):
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_queue SET status = %s, updated_at = NOW() WHERE id = %s",
                (status, queue_id)
            )
    db.execute_with_retry(_do)


# --- scrape_progress ---

def get_progress(db, source_id):
    def _do(conn):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM scrape_progress WHERE source_id = %s", (source_id,))
            row = cur.fetchone()
            if row:
                return dict(row)
            # 不存在则创建
            cur.execute("""
                INSERT INTO scrape_progress (source_id, page_details)
                VALUES (%s, '{}'::jsonb)
                RETURNING *
            """, (source_id,))
            return dict(cur.fetchone())
    return db.execute_with_retry(_do)


def update_progress(db, source_id, last_completed_page=None, next_page=None,
                    total_articles=None, stop_reason=None, page_details=None):
    def _do(conn):
        sets = ["updated_at = NOW()"]
        params = []
        if last_completed_page is not None:
            sets.append("last_completed_page = %s")
            params.append(last_completed_page)
        if next_page is not None:
            sets.append("next_page = %s")
            params.append(next_page)
        if total_articles is not None:
            sets.append("total_articles = %s")
            params.append(total_articles)
        if stop_reason is not None:
            sets.append("stop_reason = %s")
            params.append(stop_reason)
        if page_details is not None:
            sets.append("page_details = %s")
            params.append(psycopg2.extras.Json(page_details))
        params.append(source_id)
        with conn.cursor() as cur:
            cur.execute(f"UPDATE scrape_progress SET {', '.join(sets)} WHERE source_id = %s", params)
    db.execute_with_retry(_do)


# --- 工具函数 ---

def get_all_sources(db):
    def _do(conn):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM sources ORDER BY id")
            return [dict(r) for r in cur.fetchall()]
    return db.execute_with_retry(_do)


def get_article_count_by_source(db):
    """返回 {slug: count} 字典"""
    def _do(conn):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.slug, COUNT(a.id)
                FROM sources s LEFT JOIN articles a ON s.id = a.source_id
                GROUP BY s.slug ORDER BY s.slug
            """)
            return {row[0]: row[1] for row in cur.fetchall()}
    return db.execute_with_retry(_do)


def backfill_content_text(db, batch_size=100):
    """
    回填已有文章的 content_text（对 content_text 为空但 content_html 非空的记录）。
    用于数据库中已有数据但 content_text 尚未生成的情况。
    """
    total = 0
    while True:
        def _fetch(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, content_html FROM articles
                    WHERE content_text = '' AND content_html != ''
                    LIMIT %s
                """, (batch_size,))
                return cur.fetchall()

        rows = db.execute_with_retry(_fetch)
        if not rows:
            break

        for article_id, content_html in rows:
            text = html_to_text(content_html)
            def _update(conn, aid=article_id, txt=text):
                with conn.cursor() as cur:
                    cur.execute("UPDATE articles SET content_text = %s WHERE id = %s", (txt, aid))
            db.execute_with_retry(_update)
            total += 1

        log.info(f"已回填 {total} 篇 content_text")

    return total
