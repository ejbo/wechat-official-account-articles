"""
轻量级本地服务器：提供文章数据 API 和前端页面
支持文件模式和数据库模式

用法:
  python server.py          文件模式（读取 data/*.jsonl）
  python server.py --db     数据库模式（读取 PostgreSQL）
"""

import json
import os
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

DATA_DIR = "data"
PORT = 8080

USE_DB = "--db" in sys.argv

# 数据库连接（懒初始化）
_db = None


def get_db():
    global _db
    if _db is None:
        from database import Database
        _db = Database()
        _db.connect()
        _db.init_schema()
    return _db


class Handler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        """静音图片请求的日志，只打印 API 和页面请求"""
        msg = format % args
        if "/data/images/" not in msg and "/api/images/" not in msg:
            sys.stderr.write(f"{self.log_date_time_string()} {msg}\n")

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/articles":
            self.handle_articles(parse_qs(parsed.query))
        elif parsed.path.startswith("/api/images/"):
            self.handle_image(parsed.path)
        elif parsed.path == "/api/sources":
            self.handle_sources()
        elif parsed.path == "/" or parsed.path == "/index.html":
            self.serve_file("index.html", "text/html")
        else:
            super().do_GET()

    def handle_articles(self, params):
        if USE_DB:
            self._handle_articles_db(params)
        else:
            self._handle_articles_file(params)

    def _handle_articles_file(self, params):
        slug = params.get("slug", [None])[0]
        if not slug:
            files = [f for f in os.listdir(DATA_DIR)
                     if f.endswith(".jsonl") and not f.endswith("_queue.jsonl")]
            if not files:
                self.send_json({"articles": [], "total": 0, "slugs": []})
                return
            slug = files[0].replace(".jsonl", "")

        filepath = os.path.join(DATA_DIR, f"{slug}.jsonl")
        articles = []
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        articles.append(json.loads(line))

        slugs = [f.replace(".jsonl", "") for f in os.listdir(DATA_DIR)
                 if f.endswith(".jsonl") and not f.endswith("_queue.jsonl")]

        self.send_json({"articles": articles, "total": len(articles), "slug": slug, "slugs": slugs})

    def _handle_articles_db(self, params):
        from database import (get_all_sources, get_or_create_source,
                              get_articles, count_articles)
        db = get_db()

        slug = params.get("slug", [None])[0]
        limit = int(params.get("limit", [100])[0])
        offset = int(params.get("offset", [0])[0])

        sources = get_all_sources(db)
        slugs = [s["slug"] for s in sources]

        if not slug and sources:
            slug = sources[0]["slug"]

        if not slug:
            self.send_json({"articles": [], "total": 0, "slugs": []})
            return

        source = get_or_create_source(db, slug)
        rows = get_articles(db, source["id"], limit=limit, offset=offset)
        total = count_articles(db, source["id"])

        # 转换为前端兼容格式
        articles = []
        for r in rows:
            articles.append({
                "article": {
                    "title": r["title"],
                    "name": r["title"],
                    "author": r["author"],
                    "publish_time": r["publish_time"],
                    "original_url": r["original_url"],
                    "image": r["cover_url"],
                    "image_local": "",
                    "is_first": r["is_first"],
                    "content_html": r["content_html"],
                },
                "meta": {
                    "slug": slug,
                    "article_id": r["article_hash"],
                    "fetched_at": r["fetched_at"].isoformat() if r["fetched_at"] else "",
                    "db_id": r["id"],
                },
            })

        self.send_json({
            "articles": articles, "total": total,
            "slug": slug, "slugs": slugs,
            "limit": limit, "offset": offset,
        })

    def handle_image(self, path):
        """从数据库读取图片: /api/images/{image_id}"""
        if not USE_DB:
            self.send_error(404, "图片 API 仅在 --db 模式可用")
            return

        try:
            image_id = int(path.split("/")[-1])
        except (ValueError, IndexError):
            self.send_error(400, "无效的图片 ID")
            return

        from database import get_image
        db = get_db()
        img = get_image(db, image_id)

        if not img or not img.get("data"):
            self.send_error(404, "图片不存在")
            return

        data = bytes(img["data"])
        mime = img.get("mime_type", "image/jpeg")

        try:
            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "public, max-age=86400")
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    def handle_sources(self):
        """返回所有数据来源列表"""
        if not USE_DB:
            slugs = [f.replace(".jsonl", "") for f in os.listdir(DATA_DIR)
                     if f.endswith(".jsonl") and not f.endswith("_queue.jsonl")]
            self.send_json({"sources": [{"slug": s} for s in slugs]})
            return

        from database import get_all_sources, get_article_count_by_source
        db = get_db()
        sources = get_all_sources(db)
        counts = get_article_count_by_source(db)
        for s in sources:
            s["article_count"] = counts.get(s["slug"], 0)
            s["created_at"] = s["created_at"].isoformat() if s.get("created_at") else ""
            s["updated_at"] = s["updated_at"].isoformat() if s.get("updated_at") else ""
        self.send_json({"sources": sources})

    def send_json(self, data):
        try:
            body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    def serve_file(self, filename, content_type):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                body = f.read().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_error(404)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    mode = "数据库" if USE_DB else "文件"
    server = HTTPServer(("127.0.0.1", PORT), Handler)
    print(f"服务已启动: http://127.0.0.1:{PORT} (模式: {mode})")
    server.serve_forever()
