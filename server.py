"""
轻量级本地服务器：提供文章数据 API 和前端页面
用法: python server.py
"""

import json
import os
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

DATA_DIR = "data"
PORT = 8080


class Handler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        """静音图片请求的日志，只打印 API 和页面请求"""
        msg = format % args
        if "/data/images/" not in msg:
            sys.stderr.write(f"{self.log_date_time_string()} {msg}\n")

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/articles":
            self.handle_articles(parse_qs(parsed.query))
        elif parsed.path == "/" or parsed.path == "/index.html":
            self.serve_file("index.html", "text/html")
        else:
            super().do_GET()

    def handle_articles(self, params):
        slug = params.get("slug", [None])[0]
        if not slug:
            # 自动找第一个 jsonl 文件
            files = [f for f in os.listdir(DATA_DIR) if f.endswith(".jsonl")]
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

        # 可用的 slug 列表
        slugs = [f.replace(".jsonl", "") for f in os.listdir(DATA_DIR) if f.endswith(".jsonl")]

        self.send_json({"articles": articles, "total": len(articles), "slug": slug, "slugs": slugs})

    def send_json(self, data):
        try:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
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
    server = HTTPServer(("127.0.0.1", PORT), Handler)
    print(f"服务已启动: http://127.0.0.1:{PORT}")
    server.serve_forever()
