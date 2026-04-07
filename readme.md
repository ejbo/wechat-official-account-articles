# jiqizhixin-scraper

今天看啥 (jintiankansha) 专栏文章抓取器，用于抓取微信公众号文章全文及图片。

支持两种存储模式：
- **文件模式**（默认）：数据保存为 JSONL + 本地图片文件
- **数据库模式**（`--db`）：数据存入 PostgreSQL，图片以二进制存储在数据库中

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 配置：复制 .env 模板并填入你的参数
cp .env.example .env
# 编辑 .env 填入 API 凭证和数据库信息

# 文件模式 — 抓取第 1 页看效果
python scraper.py

# 数据库模式 — 初始化表结构
python init_db.py

# 启动本地预览服务器
python server.py
# 浏览器打开 http://127.0.0.1:8080
```

## 配置

所有敏感参数通过项目根目录的 `.env` 文件管理（已在 `.gitignore` 中排除）：

```bash
# 爬虫 API
JTKSHA_USER=your_email@example.com
JTKSHA_TOKEN=your_token_here
JTKSHA_SLUG=pJMG8ZXFLd

# PostgreSQL（仅 --db 模式需要）
DB_HOST=localhost
DB_PORT=5432
DB_NAME=wechat_articles
DB_USER=postgres
DB_PASSWORD=postgres
```

## 多公众号管理

在 `scraper.py` 顶部的 `SOURCES` 字典里添加公众号，每个 slug 可单独配置名称和单次运行的 API 额度：

```python
SOURCES = {
    "pJMG8ZXFLd": {"name": "机器之心", "pages_per_run": 10},
    "another_slug": {"name": "另一公众号", "pages_per_run": 5},
}
```

不指定 slug 时，`--db fetch` / `--db scrape` 会遍历 `SOURCES` 中的所有公众号。

## 命令一览

### 文件模式（默认）

| 命令 | 说明 |
|------|------|
| `python scraper.py` | 抓取第 1 页（30 篇），用于测试 |
| `python scraper.py 3` | 抓取第 3 页 |
| `python scraper.py fetch` | 从 API 获取文章列表存入队列（可持续运行） |
| `python scraper.py fetch 5` | 从第 5 页开始获取 |
| `python scraper.py scrape` | 从队列读取并爬取微信正文（可与 fetch 同时运行） |
| `python scraper.py list 2` | 仅获取第 2 页文章列表，不爬取正文 |
| `python scraper.py status` | 查看当前抓取进度 |
| `python server.py` | 启动本地 Web 服务器预览文章 |

### 数据库模式（加 `--db`）

| 命令 | 说明 |
|------|------|
| `python init_db.py` | 创建数据库表结构（自动迁移旧表） |
| `python init_db.py migrate` | 建表 + 将现有 JSONL 数据导入数据库 |
| `python init_db.py migrate --images` | 建表 + 导入数据 + 导入本地图片到数据库 |
| `python init_db.py backfill` | 为已有文章回填 `content_text` 纯文本字段 |
| `python scraper.py --db fetch` | 遍历 SOURCES 所有公众号，各自按 `pages_per_run` 限额抓取列表 |
| `python scraper.py --db fetch pJMG8ZXFLd` | 仅抓取指定 slug |
| `python scraper.py --db fetch pJMG8ZXFLd 5` | 指定 slug 从第 5 页开始 |
| `python scraper.py --db scrape` | 从数据库队列爬取正文 + 图片（所有公众号） |
| `python scraper.py --db scrape pJMG8ZXFLd` | 仅爬取指定 slug |
| `python scraper.py --db status` | 查看所有公众号的进度 |
| `python scraper.py --db status pJMG8ZXFLd` | 查看指定 slug 的进度 |
| `python server.py --db` | 启动 Web 服务器（从数据库读取） |

## 数据库设计

数据库模式使用 PostgreSQL，表结构兼容 Django ORM 命名规范，可直接用 `inspectdb` 生成 models。

```
sources              数据来源（公众号/专栏，支持多来源）
  ├── articles       文章主表（标题、作者、正文 HTML、纯文本等）
  │     └── images   图片表（BYTEA 二进制存储，区分封面/正文）
  ├── scrape_queue   爬取队列（断点续爬，status 标记进度）
  └── scrape_progress 翻页进度（断点续接 fetch）
```

**关键特性：**
- `sources` 表支持多公众号，以后加新来源只需一条记录
- `images` 通过 `article_id` 外键关联文章，前端通过 `/api/images/{id}` 获取
- `scrape_queue` 的 `status` 字段实现断点续爬（pending → processing → done/failed）
- `articles.original_url` 唯一约束保证去重

## 文件模式数据结构

```
data/
  {slug}.jsonl                  # 文章数据（每行一条 JSON 记录）
  {slug}_queue.jsonl            # 待爬取队列
  {slug}_progress.json          # 抓取进度
  images/{slug}/
    {article_id}/               # 文章 ID（URL 的 MD5 前 10 位）
      cover.jpg                 # 封面图
      1.png                     # 正文图片（按顺序编号）
      2.gif
      ...
```

### JSONL 记录结构

```json
{
  "article": {
    "title": "文章标题",
    "name": "文章名称",
    "author": "专栏名称",
    "publish_time": "20260324093032",
    "original_url": "http://mp.weixin.qq.com/s?...",
    "image": "原始封面图 URL",
    "image_local": "/data/images/{slug}/{id}/cover.jpg",
    "content_html": "正文 HTML（图片链接已替换为本地路径）"
  },
  "meta": {
    "slug": "pJMG8ZXFLd",
    "article_id": "a3b2c1d4ef",
    "fetched_at": "2026-03-24T00:04:55.374600"
  }
}
```

## 防护机制

- **去重**：基于 `original_url` 自动跳过已抓取的文章（文件模式读集合，数据库模式用 UNIQUE 约束）
- **断点续爬**：文件模式写进度文件，数据库模式用 `scrape_queue` + `scrape_progress` 表
- **断点重连**：数据库模式支持连接池 + 自动重试，网络抖动不丢数据
- **API 额度保护**：`pages_per_run` 限制每次运行消耗的翻页次数，多次运行逐步推进
- **历史缺口检测**：遇到连续已有页时，若下一页从未抓过则继续探查，不会遗漏历史缺口
- **API 限流保护**：API 返回异常时立即停止，不浪费请求次数
- **微信反检测**：每篇文章间隔 3~6 秒随机延迟，携带完整浏览器 Headers
- **连续失败熔断**：连续 5 次爬取失败自动中止

## 前端预览

`server.py` 提供本地 Web 服务器（默认端口 8080），功能包括：

- 文章列表卡片展示（封面图 + 标题 + 时间）
- 搜索过滤（按标题/作者）
- 时间排序（最新/最早）
- 点击查看文章全文（图片从本地或数据库加载）
- 查看原文链接 / 复制链接
- 深色/浅色模式切换
- 分页浏览
- 响应式布局（支持移动端）
- `--db` 模式新增 `/api/sources` 和 `/api/images/{id}` 接口

## 后续用途

抓取的数据可用于：

1. 提供给大模型训练（`articles.content_text` 纯文本字段）
2. Django 项目直接对接数据库（`inspectdb` 生成 models）
3. 全量导入分析（JSONL 格式可直接 `pandas.read_json(path, lines=True)`）
4. 生成静态 HTML 页面或前端展示
